# Cache-Aware Router Architecture

/goal

This document is complete when a maintainer can review the initial design for a
cache-aware OpenAI-compatible router for multi-node AMD Strix Halo serving
without mistaking it for an implemented production service or verified
distributed-cache result.

## Status

Production multi-node architecture and implementation plan. A one-node
OpenAI-compatible endpoint MVP now exists and is documented separately in
`docs/architecture/cache-router-openai-endpoint.md`; do not mistake that MVP
for production multi-node routing, tenant isolation, or cross-node cache
correctness.

The endpoint MVP proves one worker can accept OpenAI-compatible requests through
a cache-router daemon, build a router-owned durable blob, hydrate a missing
worker-local hot slot, restore it after a worker restart, and serve a
suffix-only route with large prompt-processing reduction. Runtime distributed
cache, multi-worker scheduling, and cross-node portability remain gated on the
upstream cache primitive audit, isolated slot restore correctness tests, tenant
policy review, and second-node validation plan.

This plan extends `docs/architecture/distributed-cache.md`. It keeps the same
boundary:

- Worker: `llama-server` / `llama.cpp` inference and model-specific cache
  correctness.
- Sidecar: node-local cache inventory, hydration, upload, checksum
  verification, eviction, and disk accounting.
- Router: OpenAI-compatible ingress, request normalization, scheduling, cache
  lookup, tenant policy, failover, and decision logging.
- Registry/store: durable metadata and immutable cache blob storage.

The router must not assume CachyLLama patches are required. First audit upstream
`llama.cpp` primitives and prove behavior with isolated probes.

## Existing Evidence Boundary

Current package evidence supports these limited statements:

- upstream RADV Step 3.7 append-heavy prompt-cache behavior has single-node
  evidence;
- branch/retry reuse has evidence only for the tested checkpoint-heavy profile;
- MTP service and micro evidence are exploratory, not speed or cache-correctness
  proof;
- a one-node OpenAI-compatible router endpoint can drive a suffix-route slot
  restore through llama.cpp and a router-owned durable blob store;
- upstream slot save/restore and CachyLLama SSD-cache behavior are source-mapped
  and planned, not behavior-proven.

Do not claim distributed cache, cross-node restore, production cache-aware
routing, tenant-safe reuse, or semantic KV correctness until the relevant plans
are implemented and validated.

## Upstream Capability Audit Gate

Before router implementation, audit current upstream `llama.cpp` and any
CachyLLama reference lane for:

| primitive | audit question |
|---|---|
| `--cache-prompt` | What prompt-prefix reuse can the stable upstream server already provide? |
| `--cache-reuse` | What are the exact semantics for nonzero reuse and similarity thresholds? |
| `--ctx-checkpoints` | What branch/retry reuse exists without SSD state? |
| `--cache-ram` | How does the RAM cache pool evict under pressure? |
| `--cache-idle-slots` / `--no-cache-idle-slots` | What slot state is retained, cleared, or discarded? |
| `--slot-save-path` | Can slot state be externally managed without patching? |
| `/slots/{id}?action=save` | Can a sidecar save a slot safely, with bounded files and metadata? |
| `/slots/{id}?action=restore` | Can an isolated worker restore and continue without full prefill? |
| router/model-management mode | Does upstream already offer useful model routing, or should routing stay external? |
| recurrent-state checkpoints | Are hybrid, recurrent, or SWA states fully represented in saved cache artifacts? |
| MTP/speculative decoding | Do MTP and speculative-draft state change save/restore compatibility? |
| Vulkan/RADV versus ROCm/HIP | Which cache artifacts are backend-compatible, if any? |

Classify CachyLLama pieces as `already-upstream`, `external-wrapper`,
`small-patch`, `experimental-lane`, or `do-not-port` before implementation.
Unknown remains unknown; it must not become a best-effort restore.

## Router Responsibilities

The router should:

- accept OpenAI-compatible `/v1/chat/completions`;
- optionally accept `/v1/completions` for prompt-style workloads;
- normalize tenant, conversation, request, and cache-scope metadata;
- ask a worker or fingerprint service for authoritative token and cache
  fingerprints instead of reimplementing tokenization;
- query the registry for compatible cache manifests;
- prefer workers with hot local cache copies when policy and compatibility
  allow;
- trigger sidecar hydration when a durable blob exists but is not local;
- fall back to cold prefill when no safe cache exists;
- enforce tenant and cache-scope policy before lookup, restore, or commit;
- record routing decisions, cache hits, misses, restore failures, fallback
  reasons, and timing metrics;
- prefer cache-aware placement over load-only placement when doing so is safe.

## Non-Responsibilities

The router should not:

- own canonical tokenization unless it links to the exact same `libllama` build
  and compatibility key as the target worker;
- interpret raw KV cache internals;
- reuse blobs across incompatible model, backend, build, tokenizer, template,
  context, KV, checkpoint, MTP, or tenant states;
- share arbitrary user prompt cache across tenants;
- publish cache metadata before durable blob verification;
- become a long-lived llama.cpp fork surface;
- store secrets, raw private prompts, unredacted user content, raw cache blobs,
  or full server logs in public artifacts.

## Proposed APIs

These are proposed interfaces for the MVP. They are not implemented in this
package.

Worker-facing APIs:

| API | purpose | notes |
|---|---|---|
| `POST /cache/fingerprint` | Return authoritative token/cache fingerprint for a request prefix | Must use the same tokenizer, chat template, tools schema, and runtime settings as inference |
| `POST /cache/restore` | Restore a compatible cache artifact into a slot/session | Must reject mismatched or unvalidated manifests |
| `POST /cache/commit` | Ask worker/sidecar to persist a validated cache point | Must return content-address and key fields only after verification |
| `GET /cache/stats` | Return cache counters and current slot/cache state | For observability and routing, not public claims by itself |
| `POST /cache/validate-restore` | Optional deterministic restore validation | Used in probes and production canaries |

Sidecar-facing APIs:

| API | purpose |
|---|---|
| `GET /inventory` | Return local cache artifact inventory and disk accounting |
| `POST /hydrate` | Fetch durable blob to local NVMe after policy/lease approval |
| `POST /upload` | Upload verified blob to durable storage |
| `POST /evict` | Evict local cache artifacts under policy |
| `POST /verify` | Rehash and validate local artifact bytes |
| `GET /health` | Sidecar readiness, disk pressure, and queue status |

Registry operations:

| operation | purpose |
|---|---|
| lookup compatible manifest | Find manifests whose strict key exactly matches the request fingerprint and policy scope |
| publish manifest | Make a verified cache blob visible after durable write |
| acquire lease | Prevent duplicate hydration, restore, or publish races |
| release lease | Complete or abort an in-flight operation |
| update residency | Record which nodes have hot local copies |
| mark blob corrupt | Remove a bad blob from routing and trigger inspection |
| expire/delete tenant scope | Enforce retention, deletion, and GC policy |
| audit cache access | Record lookup, hit, miss, restore, commit, and fallback decisions |

Decision and validation event contracts:

| artifact | purpose |
|---|---|
| `schemas/cache-router/cache-decision-event.schema.json` | Redacted per-phase router decision event for lookup, placement, restore, fallback, commit, and completion attribution |
| `schemas/cache-router/cache-validation-result.schema.json` | Redacted restore or artifact validation result with fallback, quarantine, correctness, and security signals |
| `schemas/cache-router/cache-policy.schema.json` | Minimal privacy-first policy contract for replay fixtures and future router prototypes |
| `docs/architecture/examples/cache-router-decision-trace.jsonl` | Mocked decision trace for cold prefill, hot local hit, and failed restore fallback scenarios |
| `docs/architecture/examples/cache-router-validation-results.jsonl` | Mocked validation rows showing one passing restore and one failed restore that requires fallback |
| `docs/architecture/examples/negative/cache-router-negative-fixtures.jsonl` | Mocked mutation fixtures that must be rejected for raw privacy fields, unsafe cache identity, failed fallback, or quarantine-policy mistakes |
| `docs/architecture/examples/negative/cache-router-strict-key-negative-fixtures.jsonl` | Mocked strict-key, policy, and worker-capability mismatch fixtures that must never produce restore |
| `scripts/validate_cache_router_contracts.py` | Standard-library-only offline validator for schema shape, privacy invariants, fail-closed cache use, restore validation, and negative fixtures |
| `docs/architecture/examples/replay/*.jsonl` | Synthetic request, worker, registry, policy, positive manifest/worker, expected-decision, and emitted replay-event fixtures |
| `scripts/replay_cache_router_decisions.py` | Standard-library-only offline replay harness that emits cache-decision events from synthetic fixtures |
| `docs/architecture/examples/router-prototype/*.jsonl` | Synthetic ranking cases, golden decisions, and emitted mocked decision events for the offline cache-router prototype |
| `scripts/cache_router_offline_prototype.py` | Standard-library-only mocked registry/ranking prototype that validates decisions against the same event contracts |
| `docs/reference/cache-restore-correctness-plan.md` | Retained one-node restore correctness gates, positive/negative cases, metadata, and artifact policy |
| `docs/reference/second-node-cache-validation-plan.md` | Retained cross-node validation plan for a second equivalent Strix Halo machine |

These records support reproducibility, benchmark analysis, cache-hit
attribution, restore correctness review, privacy audits, and fallback debugging.
They are not request transcripts. They must use hashes, bounded summaries,
status fields, and basis fields instead of raw prompts, raw tenant identifiers,
raw conversation identifiers, raw cache paths, raw `/slots`, raw `/metrics`, or
environment values.

Run `python3 scripts/validate_cache_router_contracts.py` before any runtime
prototype. The validator does not build or run a router; it parses the positive
mocked JSONL examples, rejects the negative privacy/fail-closed fixtures, and
checks project-specific invariants that are intentionally stricter than JSON
Schema shape alone.

Run `python3 scripts/replay_cache_router_decisions.py --json` to exercise the
offline decision replay. It consumes only synthetic request fingerprints,
worker inventory, registry rows, and policy fixtures. It proves that the
documented ranking and fail-closed cases can emit valid decision events; it does
not hydrate caches, restore slots, call `/slots`, rank real workers, or prove
runtime performance.

Run `python3 scripts/cache_router_offline_prototype.py --json` to exercise the
next mocked registry/ranking layer. It reuses the same synthetic replay fixtures,
adds golden ranking cases, and emits the same redacted decision-event contract.
It is not a network router and it does not prove llama.cpp slot or KV cache
restore correctness.

The current golden cases cover hot-local priority, durable hydration before
cold prefill, unavailable-worker fallback, stale local residency, equal-cache
capacity filtering, policy denial over a cache hit, restore-validation
quarantine/fallback, private-disabled scope, and strict compatibility mismatch.
They prove only offline decision semantics and contract emission; they do not
prove runtime KV restore correctness, sidecar hydration, production routing, or
multi-node cache reuse.

Run
`python3 scripts/validate_cache_router_contracts.py --replay-fixtures docs/architecture/examples/replay --json`
to validate replay inputs before any router prototype consumes them. This checks
synthetic request, worker, registry, policy, positive manifest/worker fixtures,
mocked replay output, and strict-key negative fixtures. A missing or mismatched
strict key, tenant/scope mismatch, unavailable worker, missing restore support,
or missing hydration support must produce a miss, denial, capacity rejection, or
cold-prefill fallback. These offline checks prove fixture semantics and privacy
hygiene only; runtime KV correctness remains gated on upstream capability audit
and isolated restore tests.

Router public/admin APIs:

| API | access | purpose |
|---|---|---|
| `POST /v1/chat/completions` | public ingress | OpenAI-compatible chat |
| `POST /v1/completions` | optional public ingress | Completion-style workloads |
| `GET /router/cache/stats` | admin only | Aggregate hit/miss/restore/fallback metrics |
| `GET /router/workers` | admin only | Worker health, model, backend, cache capability, and load |
| `GET /router/decisions` | admin only | Recent redacted decision log |

## Cache Scopes

Required scopes:

| scope | policy |
|---|---|
| `global_system` | Operator-controlled system prompts/tool schemas only; disabled unless explicitly allowlisted |
| `tenant` | Reusable only inside one tenant namespace |
| `conversation` | Reusable only inside one explicit conversation/session |
| `private_disabled` | No persistence or reuse beyond request-local execution |

Default policy:

- no cross-tenant KV reuse;
- no anonymous global user-content reuse;
- global reuse only for operator-approved system/tool prefixes;
- tenant deletion removes registry entries and schedules durable blob GC;
- cache access is auditable;
- side-channel cache-hit probing is treated as a security risk.

## Strict Cache Key

A missing or mismatched key field is a cache miss. It must never produce a
best-effort restore.

Required fields:

```text
model_id
model_architecture
gguf_tensor_manifest_hash
model_hash
tokenizer_hash
chat_template_effective_hash
special_token_policy
llama_cpp_source_commit
llama_cpp_cache_abi_version
patchset_id
build_backend
gpu_backend_driver
kv_unified_mode
ctx_size
ctx_checkpoints_config
cache_type_k
cache_type_v
flash_attention_mode
rope_freq_base
rope_freq_scale
yarn_or_rope_scaling_metadata
reasoning_format
jinja_template_mode
mtp_enabled
spec_draft_model_hash
spec_draft_config
n_parallel
n_seq_max
token_prefix_hash
prefix_token_ids_hash
n_tokens
scope
tenant_hash
conversation_hash
```

For strict router lookup, `unknown`, `not_captured`, and `not_interpreted` are
invalid. Those values are acceptable only in exploratory result evidence, where
they block stronger claims.

## Registry Schema Sketch

Recommended storage:

- Postgres as the authoritative metadata registry.
- Redis only as an optional hot lookup cache.
- Object store, MinIO, S3, or a shared filesystem for immutable blobs.
- SQLite only for node-local sidecar inventory.

Draft SQL sketch:

```sql
create table workers (
  worker_id text primary key,
  node_id text not null,
  base_url text not null,
  runtime_commit text not null,
  build_backend text not null,
  gpu_backend_driver text not null,
  health_status text not null,
  last_seen_at timestamptz not null
);

create table worker_capabilities (
  worker_id text references workers(worker_id),
  model_id text not null,
  model_hash text not null,
  supports_fingerprint boolean not null,
  supports_restore boolean not null,
  supports_commit boolean not null,
  supports_slot_save_restore boolean not null,
  supports_mtp boolean not null,
  max_ctx_size integer not null,
  cache_artifact_kinds jsonb not null,
  primary key (worker_id, model_id, model_hash)
);

create table cache_manifests (
  manifest_id text primary key,
  schema_version text not null,
  cache_key_hash text not null unique,
  cache_artifact_kind text not null,
  scope text not null,
  tenant_hash text not null,
  conversation_hash text,
  token_prefix_hash text not null,
  n_tokens integer not null,
  model_id text not null,
  model_hash text not null,
  tokenizer_hash text not null,
  chat_template_effective_hash text not null,
  llama_cpp_source_commit text not null,
  llama_cpp_cache_abi_version text not null,
  build_backend text not null,
  gpu_backend_driver text not null,
  kv_key jsonb not null,
  mtp_key jsonb not null,
  validation_status text not null,
  content_address text not null,
  size_bytes bigint not null,
  created_at timestamptz not null,
  expires_at timestamptz
);

create table cache_blob_parts (
  manifest_id text references cache_manifests(manifest_id),
  part_index integer not null,
  content_address text not null,
  size_bytes bigint not null,
  sha256 text not null,
  primary key (manifest_id, part_index)
);

create table cache_residency (
  manifest_id text references cache_manifests(manifest_id),
  worker_id text references workers(worker_id),
  local_path_hash text not null,
  verified_at timestamptz not null,
  last_hit_at timestamptz,
  bytes_on_nvme bigint not null,
  primary key (manifest_id, worker_id)
);

create table cache_locks (
  lock_id text primary key,
  manifest_id text references cache_manifests(manifest_id),
  worker_id text references workers(worker_id),
  operation text not null,
  acquired_at timestamptz not null,
  expires_at timestamptz not null
);

create table cache_access_log (
  event_id text primary key,
  captured_at timestamptz not null,
  tenant_hash text not null,
  conversation_hash text,
  worker_id text,
  route_decision text not null,
  cache_event text not null,
  fallback_reason text,
  manifest_id text,
  request_hash text not null,
  decision_latency_ms numeric,
  restore_latency_ms numeric
);

create table cache_policy (
  policy_id text primary key,
  scope text not null,
  tenant_hash text,
  allow_global_system boolean not null,
  persist_user_content boolean not null,
  ttl_seconds integer,
  max_bytes bigint,
  encryption_required boolean not null
);
```

## Blob Lifecycle

Required lifecycle:

1. Worker computes cache blob or slot state.
2. Worker/sidecar writes a temp file under a bounded local directory.
3. File is flushed and `fsync` completes.
4. Blob hash and size are computed.
5. Sidecar verifies the artifact against the strict key and manifest.
6. Blob is uploaded to durable storage under a content-addressed key.
7. Registry transaction publishes the manifest only after blob verification.
8. Router can route future requests to a hot local copy or request hydration.
9. Corrupt, truncated, missing, or policy-invalid blobs are rejected.
10. GC removes expired blobs only after registry and tenant policy allow it.

## Routing Algorithm

Request flow:

```text
receive OpenAI-compatible request
determine tenant, conversation, requested cache scope, and policy
reject or downgrade persistence if policy forbids it
select fingerprint-capable candidate worker for the model/backend lane
ask worker for authoritative token/cache fingerprint
query registry for exact compatible manifest
rank candidates:
  1. exact hot local cache hit on healthy worker
  2. compatible durable cache available and worker can hydrate quickly
  3. same model already loaded, cold prefill required
  4. model load required
  5. reject or backpressure
acquire lease for hydration or restore when needed
restore compatible cache or run cold prefill
serve request
commit/update cache if policy and validation allow
log route decision, validation status, fallback requirement, metrics, and cache event basis
```

Failure flow:

```text
restore fails checksum, key, policy, or validation
mark manifest suspect or blob corrupt as appropriate
release lease
fall back to cold prefill if request policy allows
never return output from a corrupt-cache path
record failure reason, validation basis, security signal, quarantine decision,
and cold-prefill fallback requirement
schedule follow-up verification or GC
```

## Correctness Validation

Minimum tests before any router cache hit can support a public claim:

| test | expected result |
|---|---|
| same prompt cold vs restored next-token logits | match within documented tolerance |
| top-k distribution after restore | match cold baseline within documented tolerance |
| deterministic text generation | exact or explained match under fixed seed/sampling |
| restore at system prompt boundary | safe restore or forced recompute |
| restore mid-conversation | safe restore or forced recompute |
| restore after tool output | safe restore or forced recompute |
| restore near context limit | safe restore or forced recompute |
| restore with MTP enabled | safe restore or forced recompute with MTP fields in key |
| restore with MTP disabled | safe restore or forced recompute with non-MTP key |
| wrong model | rejected before restore |
| wrong tokenizer | rejected before restore |
| wrong chat template | rejected before restore |
| wrong tools schema | rejected before restore |
| wrong llama.cpp commit or cache ABI | rejected before restore |
| wrong backend or driver lane | rejected before restore unless proven compatible |
| wrong KV type or flash-attention mode | rejected before restore |
| wrong tenant or conversation | rejected before lookup/restore |
| corrupt, truncated, missing, or swapped blob | rejected and routed to cold prefill |
| stale residency | self-healed by verify/hydrate or removed from hot candidate set |

## Performance Benchmarks

Record these metrics for cache-router work:

| metric | meaning |
|---|---|
| cold TTFT | TTFT with no compatible cache |
| restored TTFT | TTFT after a validated restore |
| prefill tokens/sec | Prompt processing throughput for cold or partial-prefill work |
| eval tokens/sec | Decode throughput after routing/restore |
| cache lookup latency | Registry lookup and policy evaluation time |
| hydration latency | Durable blob fetch to local NVMe |
| restore latency | Worker restore operation duration |
| blob upload latency | Commit path upload duration |
| local NVMe hit rate | Requests served from verified local cache copies |
| durable cache hit rate | Requests that hydrate from durable storage |
| fallback rate | Requests that fall back to cold prefill |
| cache corruption rate | Corrupt or invalid blob detections per lookup/restore |
| routing decision latency | Router scheduling overhead |
| tail latency p50/p95/p99 | Request and decision latency distribution |
| node memory pressure | Worker and node memory before/after |
| OOM events | Hard failures or near-OOM stops |
| cache eviction count | Local sidecar evictions |
| full context reprocess count | Requests that unexpectedly reprocess the full prompt |
| erased/discarded cache events | Runtime or policy cache loss events |

Use `docs/benchmark-metric-glossary.md` and
`docs/benchmark-reporting-tables.md` for metric semantics. Do not infer cache
correctness from latency improvement alone.

## MVP Phases

Phase 0: capability audit

- Audit upstream `llama.cpp` cache primitives.
- Diff CachyLLama behavior against upstream.
- Classify each CachyLLama patch as `already-upstream`, `external-wrapper`,
  `small-patch`, `experimental-lane`, or `do-not-port`.

Phase 1: single-node local prototype

- Use upstream slot save/restore or prompt-cache files.
- Build a registry mock.
- Validate strict cache keys.
- Run cold versus restored correctness tests.

Phase 2: two-node slot-file prototype

- Node A prefill and save.
- Copy/checksum blob.
- Node B restore.
- Compare logits, top-k, and deterministic text.
- Measure TTFT change.

Phase 3: sidecar and registry

- Node-local inventory.
- Hydration/upload.
- Immutable blob storage.
- Lease and residency tracking.
- Eviction policy.

Phase 4: cache-aware router MVP

- OpenAI-compatible ingress.
- Worker registry.
- Cache lookup.
- Cache-aware placement.
- Fallback to cold prefill.
- Metrics and redacted decision logging.

Phase 5: minimal worker hooks

- Add `/cache/fingerprint`, `/cache/commit`, `/cache/restore`, and
  `/cache/stats` only if upstream primitives are insufficient.
- Keep the llama.cpp patch stack minimal.

Phase 6: production hardening

- Tenant policy.
- Encryption at rest.
- Audit log.
- GC.
- Backpressure.
- Failover.
- RPO/RTO modes.

## Durability Policy

Avoid vague "preserve cache no matter what" goals. Use explicit modes:

| mode | target |
|---|---|
| strict expensive-prefill mode | RPO 0 for completed expensive prefill after registry publish |
| periodic generation checkpoint mode | RPO <= N generated tokens, where N is configured per workload |
| restart/failover mode | RTO target for restoring a compatible worker on another node |
| degraded mode | explicit fallback to cold prefill when durability cannot be proven |

Cache acceleration must never be more important than correctness. If durability
or validation is uncertain, route to cold prefill.

## Security And Privacy

Requirements:

- KV cache and sequence-state files are sensitive data.
- Encryption at rest is mandatory for durable shared blobs.
- Per-tenant namespace is mandatory.
- Cross-tenant cache reuse is denied by default.
- Operator-controlled global prompt allowlist is mandatory for
  `global_system`.
- Cache access audit log is mandatory.
- Tenant deletion and GC path are mandatory.
- Cache-hit probing and timing side channels must be addressed.
- Public benchmark logs must not include private prompts, secrets, raw tenant
  data, raw cache blobs, or full server logs.
- Forged manifests, path traversal, partial publish, corrupt blobs, and stale
  residency must have explicit tests.

## Implementation Deliverables

Initial planning deliverables in this package:

```text
docs/architecture/cache-router.md
docs/architecture/cache-router-mvp-checklist.md
schemas/cache-router/cache-manifest.schema.json
schemas/cache-router/worker-capabilities.schema.json
schemas/cache-router/cache-policy.schema.json
schemas/cache-router/cache-decision-event.schema.json
schemas/cache-router/cache-validation-result.schema.json
docs/architecture/examples/cache-router-decision-trace.jsonl
docs/architecture/examples/cache-router-validation-results.jsonl
docs/architecture/examples/negative/cache-router-negative-fixtures.jsonl
docs/architecture/examples/negative/cache-router-strict-key-negative-fixtures.jsonl
docs/architecture/examples/replay/requests.jsonl
docs/architecture/examples/replay/workers.jsonl
docs/architecture/examples/replay/registry.jsonl
docs/architecture/examples/replay/policies.jsonl
docs/architecture/examples/replay/manifests-positive.jsonl
docs/architecture/examples/replay/workers-positive.jsonl
docs/architecture/examples/replay/expected-decisions.jsonl
docs/architecture/examples/replay/mock-router-output.jsonl
docs/architecture/examples/router-prototype/ranking-cases.jsonl
docs/architecture/examples/router-prototype/golden-decisions.jsonl
docs/architecture/examples/router-prototype/mock-router-output.jsonl
scripts/cache_router_offline_prototype.py
scripts/replay_cache_router_decisions.py
scripts/validate_cache_router_contracts.py
```

Future implementation deliverables should stay outside the production service
path until the checklist passes:

- prototype router package with mocked workers;
- registry migration files;
- sidecar inventory prototype;
- redacted decision-log schema;
- isolated upstream slot save/restore probe artifacts;
- two-node restore correctness report.

## Stop Conditions

Stop before implementation or runtime work if:

- the upstream primitive audit is stale or incomplete;
- compatibility key fields are unknown for a strict lookup;
- restore correctness cannot be measured;
- a planned test would touch a persistent production worker;
- a temporary server lacks cleanup proof;
- memory headroom is below an operator-approved threshold;
- raw slot/cache blobs or private prompts would be tracked;
- a design allows cross-tenant reuse by default;
- global user-content cache is proposed;
- the result would be marketed as production-ready before Phase 6.
