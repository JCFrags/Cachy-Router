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

When binding to a non-loopback address, the helper requires
`--allow-unauthenticated-lan` so the operator explicitly accepts the trusted-LAN
boundary. Put a reverse proxy, firewall, VPN, or authentication layer in front of
the router before any broader exposure.

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

After starting the workers and router, check the public API surface:

```bash
curl -fsS http://<router-lan-ip>:18080/health
curl -fsS http://<router-lan-ip>:18080/v1
curl -fsS http://<router-lan-ip>:18080/v1/models
curl -fsS http://<router-lan-ip>:18080/router/workers
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

`GET /v1` returns a small router metadata document that lists usable
OpenAI-compatible and router inspection endpoints. Some clients probe `/v1`
before calling `/v1/models`; that probe should look like a successful router
connection rather than a compatibility failure.

Normal `/v1/completions` and `/v1/chat/completions` requests pass through to a
healthy worker. Cache acceleration is opt-in through a nonstandard
`cache_router` object that the router removes before forwarding the request to
`llama-server`.

`POST /tokenize` is a compatibility/helper proxy to a healthy worker. It is not
an OpenAI endpoint, but it lets controller-side tests tune synthetic prompt
sizes through the router without contacting worker backends directly.

## Worker Selection

Transparent proxy responses include diagnostic headers:

- `X-Cache-Router-Worker`: selected worker ID;
- `X-Cache-Router-Worker-Availability`: slot state used for selection, such as
  `idle`, `busy`, or `slot_state_unknown`;
- `X-Cache-Router-Worker-Busy-Score`: `0` for idle, `1` for unknown, `2` for
  busy.

The headers are for operator/debug visibility and should not be treated as
OpenAI API fields.

Current scheduling is intentionally simple:

- normal OpenAI pass-through prefers a healthy idle worker when slot state is
  available;
- cache build uses the selected or requested worker and records
  `source_worker_id`;
- cache use prefers idle workers, then recorded hot local residency;
- if no hot local copy is available, the router can hydrate the durable blob
  into the selected healthy worker's local slot directory before restore;
- if a hot/resident worker is busy and another healthy worker is idle, the
  router can hydrate and continue on the idle worker when fallback is allowed.

This is not a production load balancer. Queues, quotas, weighted scheduling,
admission control, and tenant policy remain future work.

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

Streaming cached requests are not supported yet. Use `stream: false` for cache
hits.

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
      "slot_save_path": "/home/<user>/.cache/cachy-router/workers/worker-a/slots",
      "slot_id": 0,
      "model": "<model-name>",
      "model_identity": "<shared-main-model-identity>",
      "model_path": "/models/<main-model>.gguf",
      "llama_server_path": "/home/<user>/llama.cpp/build/bin/llama-server",
      "llama_server_version": "<llama.cpp-version-or-commit>",
      "ctx_size": 65536,
      "cache_type_k": "q8_0",
      "cache_type_v": "q8_0",
      "mtp_enabled": true,
      "spec_draft_model_identity": "<shared-draft-model-identity>",
      "spec_draft_model_path": "/models/<draft-model>.gguf",
      "transport": {
        "kind": "http",
        "sidecar_url": "http://<worker-a-lan-ip>:18083"
      }
    }
  ]
}
```

Worker-local paths may differ across machines. Cache compatibility depends on
shared model/runtime identities plus matching model bytes, runtime version,
context size, KV cache types, MTP/speculative settings, and template behavior.
A shared path string alone is never sufficient proof of compatibility.

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
- quota, eviction, tenant isolation, and sidecar hardening remain future work;
- cache-hit semantics are suffix-route only;
- streaming cached requests are not supported yet;
- restore correctness is timing/API based and still needs logits/top-k or
  deterministic text comparison tests before publication-quality correctness
  claims.
