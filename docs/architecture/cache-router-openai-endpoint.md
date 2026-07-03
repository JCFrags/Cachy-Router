# OpenAI Cache Router Endpoint

This document explains the OpenAI-compatible endpoint exposed by Cachy Router:
how to point a client at it, what routes are implemented, how worker selection
is reported, and how the opt-in cache extension works.

For installation and process startup, start with
`docs/architecture/cache-router-setup.md`.

## Security Boundary

The default documented mode is unauthenticated on a trusted LAN. This is convenient
for local experimentation, but it is not safe for the public internet or an
untrusted network.

Do not expose these unauthenticated services outside a trusted network:

- the router endpoint, usually port `18080`;
- worker `llama-server` endpoints, usually port `18082`;
- worker sidecars, usually port `18083`.

When binding an unauthenticated router to a non-loopback address, the daemon and
stack helper require `--allow-unauthenticated-lan` so the operator explicitly
accepts the trusted-LAN boundary. Put a reverse proxy, firewall, VPN, or
authentication layer in front of the router before any broader exposure.

## Topology

```text
OpenAI-compatible client
  -> Cachy Router endpoint on any reachable host
  -> one of N worker llama-server processes
  -> worker-local slot sidecar
  -> worker-local hot slot cache
  -> router-owned durable cache blob store
```

The router can run on any machine that can reach the worker URLs and sidecar
URLs. It may be colocated with a worker, but it can also run on a controller,
desktop, or independent aggregator.

The worker inventory is a list. Add one row per worker; there is no intended
two-worker limit.

## Client Configuration

Use these settings in an OpenAI-compatible client:

```text
Base URL: http://<router-lan-ip>:18080/v1
Model: <model-name-served-by-workers>
Authentication: none for trusted-LAN mode
```

If a client refuses to save a provider without an authentication field, use a
blank value if the UI allows it, or any harmless placeholder if it does not. The
no-key trusted-LAN path does not validate that field.

When the router is started with `--production-mode`, every `/v1/*`,
`/tokenize`, `/router/*`, and `/metrics` request requires either an
`Authorization: Bearer <token>` header or an `X-API-Key` header. `/health`
remains unauthenticated but returns reduced detail unless the request is
authorized. Production mode requires a configured token, rejects the
unauthenticated LAN override, and disables admin endpoints unless the operator
explicitly allows authenticated admin endpoints.

For local-only access, bind the router to loopback or use an SSH tunnel:

```bash
ssh -N -L 18080:127.0.0.1:18080 <router-ssh-alias>
```

Then configure the client with:

```text
Base URL: http://127.0.0.1:18080/v1
Model: <model-name-served-by-workers>
Authentication: none for local or tunneled trusted use
```

## Endpoint Checks

After starting a trusted-LAN no-auth router, check the public API surface:

```bash
curl -fsS http://<router-lan-ip>:18080/health
curl -fsS http://<router-lan-ip>:18080/v1
curl -fsS http://<router-lan-ip>:18080/v1/models
curl -fsS http://<router-lan-ip>:18080/router/workers
curl -fsS http://<router-lan-ip>:18080/metrics
```

For production mode, `/health` remains unauthenticated and reduced-detail; all
other checks need a configured bearer token or `X-API-Key`:

```bash
TOKEN='<router-token>'
curl -fsS http://<router-lan-ip>:18080/health
curl -fsS -H "Authorization: Bearer ${TOKEN}" http://<router-lan-ip>:18080/v1
curl -fsS -H "Authorization: Bearer ${TOKEN}" http://<router-lan-ip>:18080/v1/models
curl -fsS -H "Authorization: Bearer ${TOKEN}" http://<router-lan-ip>:18080/router/workers
curl -fsS -H "Authorization: Bearer ${TOKEN}" http://<router-lan-ip>:18080/metrics
```

Implemented endpoints:

- `GET /health`
- `GET /v1`
- `GET /v1/models`
- `POST /v1/completions`
- `POST /v1/chat/completions`
- `POST /tokenize`
- `GET /router/status`
- `GET /router/workers`
- `GET /router/cache`
- `GET /router/decisions`
- `GET /metrics`

`GET /v1` returns a small router metadata document that lists usable
OpenAI-compatible and router inspection endpoints. Some clients probe `/v1`
before calling `/v1/models`; that probe should look like a successful router
connection rather than a compatibility failure.

`GET /router/cache` returns a redacted admin registry summary. Cache rows expose
bounded identifiers, sizes, `validation_status`, optional `quarantine_reason`,
optional `quarantined_at`, and per-worker residency. The response does not
include raw router blob paths, worker slot paths, prompts, private hostnames, or
environment paths.

Normal `/v1/completions` and `/v1/chat/completions` requests pass through to a
ready worker. A worker is route-ready only when its process `/health` probe
passes and its worker `/v1/models` response includes the configured model.
Worker `/v1/models` HTTP 503 loading responses are tracked as warming and are
skipped while another ready worker exists. Cache acceleration is opt-in through
a nonstandard `cache_router` object that the router removes before forwarding
the request to `llama-server`.

Normal non-cached `stream: true` completion and chat completion requests are
relayed as streaming responses. The router selects a ready worker first, then
forwards backend event bytes without buffering the full response or computing a
response `Content-Length`.

Sidecar health is tracked separately from normal OpenAI route readiness. Normal
requests without `cache_router` require worker process health and worker
`/v1/models` readiness; HTTP sidecar reachability is required for cache artifact
operations such as upload, hydrate, verify, and eviction.

`POST /tokenize` is a compatibility/helper proxy to a ready worker. It is not
an OpenAI endpoint, but it lets controller-side tests tune synthetic prompt
sizes through the router without contacting worker backends directly.

## Error Shapes

Router errors use an OpenAI-shaped error object:

```json
{
  "error": {
    "message": "<bounded router message>",
    "type": "<error-type>",
    "code": 400
  }
}
```

Current bounded error `type` values include:

- `authentication_error`: missing or invalid bearer token or `X-API-Key` when
  router auth is enabled;
- `invalid_request_error`: required OpenAI request fields are absent or invalid;
- `model_not_found`: the requested model is not configured in the router
  inventory;
- `worker_not_found`: an explicit `cache_router.worker_id` is not configured;
- `service_unavailable`: no route-ready worker is available for the requested
  model, or optional router-side queueing rejected the request before backend
  forwarding because the selected worker queue was full or timed out;
- `cache_not_found`: a `mode=use` request names an absent scoped cache entry,
  including tenant, scope, or conversation-scope policy denials that are
  miss-masked;
- `cache_policy_denied`: a cache request asks for a scope the daemon does not
  admit for persistence/reuse, such as non-allowlisted `global_system` or
  `private_disabled`;
- `invalid_json`: the request body is not valid JSON;
- `not_found`: the route is unknown, or an admin route is disabled by
  `--disable-admin-endpoints`;
- `cache_router_error`: bounded catch-all for cache-router failures that do not
  fit a narrower type.

OpenAI-route errors rejected before worker selection include
`X-Cache-Router-Worker: none`. Routed backend failures include the selected
worker when one was chosen before the error.

Worker sidecars expose a separate trusted-LAN operator API on their sidecar URL:

- `GET /health`
- `GET /inventory`
- `POST /upload`
- `POST /hydrate`
- `POST /verify`
- `POST /evict`
- `POST /leases/acquire`
- `POST /leases/release`
- legacy `GET /slots`, `GET /slots/<filename>/info`,
  `GET /slots/<filename>/content`, and `PUT /slots/<filename>/content`

The named `/upload` API verifies an existing local artifact for router pull or
publishes router-provided content. The named `/hydrate` API publishes
router-provided content. Both routes verify hash and size before publishing and
never fetch arbitrary paths or URLs. Duplicate verified hydrate requests return
a no-op result when the local bytes already match. Sidecar lease APIs are local
cooperative eviction guards only; they are not a registry/store lock protocol.

## Metrics

`GET /metrics` returns Prometheus text-format counters and gauges from the
router process. When router auth is enabled, the endpoint uses the same bearer
token or `X-API-Key` checks as the admin routes.

The `/router/*` inspection routes and `/metrics` are operator/admin surfaces.
They can be protected with router auth, or disabled entirely with
`--disable-admin-endpoints`. Disabling admin endpoints leaves `/v1`,
`/v1/models`, `/v1/completions`, `/v1/chat/completions`, and `/tokenize`
available.

Current metrics are in-memory and reset when the router process restarts. They
include request and error counts, active requests, request-latency summaries,
routing-decision latency summaries, configured, process-healthy, and route-ready
worker counts, per-worker readiness, per-worker selection counts, cache
decision-event counts, and optional queueing metrics. Queueing metrics include
per-worker queue depth, per-worker active capacity, configured queue limit,
queue wait summaries, and queue rejection counters. Queueing is disabled by
default when `--queue-limit-per-worker` is `0`.

## Worker Selection

Routed proxy responses include diagnostic headers:

- `X-Cache-Router-Request-ID`: router-owned request ID for correlating the
  response with `/router/decisions`;
- `X-Cache-Router-Worker`: selected worker ID;
- `X-Cache-Router-Worker-Availability`: slot state used for selection, such as
  `idle`, `busy`, or `slot_state_unknown`;
- `X-Cache-Router-Worker-Busy-Score`: `0` for idle, `1` for unknown, `2` for
  busy.

When queueing is enabled, scheduler decision events also include each
candidate's `queue_depth` and `queue_capacity`, and the rank fields record that
queue depth was part of the normal proxy routing decision. Queue-full and
queue-timeout rejections happen before backend forwarding and report
`X-Cache-Router-Worker: none`.

Successful cache-extension responses include `X-Cache-Router-Request-ID`,
`X-Cache-Router-Trace-ID`, and `X-Cache-Router-Worker`. Successful cache-use
responses also include `X-Cache-Router-Cache-Hit-Level` with the bounded hit
classification used for that request. Policy-denied cache lookups are not
successful cache-use responses: they are denied before worker selection,
hydrate, restore, or suffix generation, and the client-visible response is
masked as the same scoped cache-miss shape used for an absent cache. Admin-only
decision logs may still record `reject_policy` / `policy_denied` for audit.

Router-generated responses that do not select a worker, including `/health`,
`/metrics`, `/v1` discovery, admin inspection responses, authentication
errors, invalid JSON errors, disabled-admin 404s, and OpenAI-route errors that
are rejected before worker selection, use `X-Cache-Router-Worker: none`
alongside router-owned request and trace IDs.

The headers are for operator/debug visibility and should not be treated as
OpenAI API fields. Client-supplied `X-Cache-Router-*` headers are ignored by
the router and are not forwarded to workers.

`GET /router/decisions` exposes recent JSONL decision events. Add
`?request_id=<id>` to filter to one response, and `&limit=<n>` to adjust the
bounded tail size. Normal pass-through events include the same router-owned
request ID as the response header, selected worker, decision phase, fallback
reason when fallback happened, and routing-decision timing. Cache-extension
events use the same router-owned request and trace IDs as the response. Events
also include a bounded scheduler trace with candidate worker IDs, readiness
state, availability reason, busy score, in-flight request count, rank tuple,
selected worker, and winner reason. Events use hashes and bounded summaries,
not raw prompts.

Current scheduling is intentionally simple:

- normal OpenAI pass-through prefers a ready idle worker when slot state is
  available, then lower per-worker in-flight request count;
- equal-score cold pass-through traffic rotates across ready workers instead of
  pinning one inventory row forever;
- cache build uses the selected or requested worker and records
  `source_worker_id`;
- cache use prefers idle workers, then recorded hot local residency;
- if no hot local copy is available, the router can hydrate the durable blob
  into the selected ready worker's local slot directory before restore;
- if a hot/resident worker is busy and another ready worker is idle, the
  router can hydrate and continue on the idle worker when fallback is allowed.

This is not a production load balancer. Quotas, weighted scheduling, admission
control, auth-derived tenant isolation, and production tenant policy are
outside the current trusted-LAN MVP claim.

## Cache Extension

Cache acceleration is explicit. Send normal OpenAI-compatible requests without
`cache_router` for transparent pass-through.

Completion cache build:

```json
{
  "model": "<model-name-served-by-workers>",
  "prompt": "",
  "max_tokens": 1,
  "temperature": 0,
  "cache_router": {
    "mode": "refresh",
    "cache_id": "demo-prefix",
    "prefix_text": "<long reusable prefix>",
    "suffix_text": "",
    "target": "suffix_route"
  }
}
```

Completion cache use:

```json
{
  "model": "<model-name-served-by-workers>",
  "prompt": "Answer with exactly: router cache ok",
  "max_tokens": 16,
  "temperature": 0,
  "cache_router": {
    "mode": "use",
    "cache_id": "demo-prefix",
    "suffix_text": "Answer with exactly: router cache ok",
    "target": "suffix_route"
  }
}
```

Supported modes:

- `bypass`: transparent proxy, no cache action;
- `build`: build prefix cache and publish a durable blob;
- `use`: hydrate/restore existing cache and send only suffix text;
- `auto`: use existing cache or build from `prefix_text` on miss;
- `refresh`: rebuild and replace the cache entry.

`cache_router.mode=bypass` requests may use `stream: true`; the router strips
the extension and relays the normal backend stream. Cached build/use/auto/refresh
requests are rejected clearly when `stream: true`. Use `stream: false` for cache
hits.

Cached build/use requests may include hashed policy and restore guard fields:

- `scope`: `conversation` for one explicit conversation/session, or `tenant`
  for the explicit broader same-tenant reuse policy in the daemon MVP;
  `conversation` is the default;
- `tenant_hash`: SHA-256 hex tenant namespace, never a raw tenant ID;
- `conversation_hash`: SHA-256 hex conversation/session namespace for
  `conversation` scope;
- `policy_id_hash`: SHA-256 hex policy identifier;
- `cache_key_hash`: optional SHA-256 hex strict-cache-key guard for
  restore-capable `use`, `auto`, and `refresh` requests. For `use` and `auto`,
  callers must supply this field or non-empty `prefix_text` so the daemon can
  derive the request/runtime strict key;
- `measure_true_ttft`: optional boolean for completion cache `use` and `auto`
  requests. When true, the daemon performs an internal one-token native
  streaming probe after slot restore, records router-observed restored TTFT in
  `cache_router.use.completion` and the decision event, and still returns the
  normal non-streaming JSON response.

The daemon scopes lookup/build by these fields. A compatible cache found under
another tenant/scope is denied before hydrate, restore, or suffix generation,
logged for admin audit, and masked to the client as `cache_not_found` without
`X-Cache-Router-Cache-Hit-Level`. A different `conversation_hash` is denied for
`scope=conversation`; `scope=tenant` omits the conversation key and is the
explicit way to allow same-tenant reuse across conversations.

When `cache_key_hash` is supplied, it is an additional fail-closed equality
guard: the request hash, scoped registry row hash, and loaded manifest
`cache_key_hash` must match before hydrate, restore, suffix generation, or
refresh rebuild work. A valid mismatching hash is treated as a cache miss or
fallback according to the existing request policy and receives no cache-hit
header; a malformed hash is an `invalid_request_error`. This is not a
substitute for manifest and worker strict compatibility checks.

When the client does not supply `cache_key_hash` for `use` or `auto`,
`prefix_text` is required. The daemon derives candidate strict keys from that
prefix, scoped policy, requested model/worker, and ready worker runtime
fingerprints, then looks up only an exact scoped `cache_id` plus
`cache_key_hash` registry row. Cache-id-only `use`/`auto` requests return
`invalid_request_error` before restore. The selected manifest's
`cache_key_hash` is also recomputed from persisted canonical key material before
hydrate or restore, which prevents registry and manifest rows from agreeing on
an arbitrary forged hash.

Cached chat acceleration is not template-safe yet. Normal chat pass-through
works, but accelerated chat should use explicit `prefix_text` and `suffix_text`
until template rendering is proven and strict-keyed.

## Worker Targeting

Requests can temporarily target a specific configured worker:

```json
{
  "cache_router": {
    "mode": "use",
    "cache_id": "demo-prefix",
    "worker_id": "worker-a"
  }
}
```

The same `worker_id` and `allow_fallback` fields can be used with
`mode=bypass` for diagnostic normal OpenAI requests. The router strips the
extension before forwarding to `llama-server` and returns the selected worker in
the `X-Cache-Router-Worker` response header.

The router defaults cached `use` requests to `allow_fallback=true`. Set
`cache_router.allow_fallback=false` only when intentionally testing one specific
worker and you want the request to fail instead of using another healthy
configured worker.

## Worker Inventory Compatibility

The daemon accepts a JSON inventory with a top-level `workers` list. The
canonical editable example is:

```text
configs/cache-router/workers.example.json
```

Example shape:

```json
{
  "workers": [
    {
      "worker_id": "worker-a",
      "worker_url": "http://<worker-a-lan-ip>:18082",
      "slot_save_path": "/home/<user>/.cache/strix-halo-cache-router/workers/worker-a/slots",
      "slot_id": 0,
      "strict_metadata_auto": true,
      "strict_metadata_force_runtime": true,
      "model": "<model-name>",
      "model_path": "/models/<main-model>.gguf",
      "llama_server_path": "/home/<user>/llama.cpp/build/bin/llama-server",
      "ctx_size": 65536,
      "kv_unified_mode": true,
      "cache_type_k": "q8_0",
      "cache_type_v": "q8_0",
      "mtp_enabled": true,
      "spec_draft_model_path": "/models/<draft-model>.gguf",
      "spec_draft_n_max": 3,
      "spec_draft_n_min": 0,
      "spec_draft_p_split": "0.10",
      "spec_draft_p_min": "0.60",
      "transport": {
        "kind": "http",
        "sidecar_url": "http://<worker-a-lan-ip>:18083"
      }
    }
  ]
}
```

Worker-local paths may differ across machines. Normal OpenAI-compatible
pass-through does not require every strict cache field, but cache build/use
does. With `strict_metadata_auto` and `strict_metadata_force_runtime` enabled,
the daemon computes those strict fields from runtime APIs before cache lookup or
build. For cache restore, the daemon fails closed when required strict fields
are missing, malformed, `unknown`, `not_captured`, or `not_interpreted`. Cache
compatibility depends on matching computed model bytes, GGUF tensor manifest
surrogate, tokenizer, chat template, tools schema, system prompt, model
architecture, special-token policy, llama.cpp source/cache ABI, local patchset,
backend/driver lane, context size, KV cache types, RoPE/YaRN scaling metadata,
MTP/speculative settings, parallel/sequence lane, and template behavior.
Explicit hand-entered strict fields are reserved for pinned/offline deployments
that disable runtime derivation.

Cache use selects by either a request-supplied `cache_key_hash` or a strict key
derived from the request prefix, scoped policy, requested model/worker, and
worker runtime fingerprint. The selected manifest is then checked against its
own canonical key material before hydrate or restore.

When `--workers-file` is used, the daemon hot-reloads the file on
`--inventory-reload-interval` seconds. The default interval is 5 seconds; set it
to `0` to disable reload. A successful reload swaps the worker set, clears
readiness cache, and makes added or changed workers eligible only after their
worker `/health` and `/v1/models` checks pass. Removed workers receive no new
routes after reload; existing in-flight requests may finish. Invalid or
partially written inventories keep the last good worker set, and `/router/status`
reports the last reload error.

The `http` transport is the preferred MVP path because it lets the router run
independently from worker nodes. The older `local` transport is only for
colocated router/worker tests. The `ssh` transport is useful in controlled
deployments but requires the router host itself to have SSH access to workers.

## Retained Evidence And Limits

Public-safe proof-of-concept summaries live in
`evidence/cache-router-results-summary.md`, with claim boundaries summarized in
`docs/benchmark-claim-map.md`. Raw run folders should stay private unless they
have been redacted and reviewed.

Current limitations:

- no-key LAN mode is only for trusted networks;
- scheduling is not a production load-balancing control plane;
- quota, production retention/GC orchestration, tenant isolation, and sidecar hardening remain
  future work;
- cache-hit semantics are suffix-route only;
- streaming cached requests are rejected rather than accelerated;
- restore correctness has an operator-run deterministic text comparison probe
  and one scoped no-MTP trusted-LAN pass, but MTP-enabled restore,
  multi-node/restart restore, and logits/top-k runtime support remain required
  before publication-quality distributed-cache correctness claims.
