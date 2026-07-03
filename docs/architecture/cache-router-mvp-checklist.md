# Cache Router MVP Checklist

/goal

This checklist is complete when a maintainer can decide whether the
cache-aware router track is ready to move from architecture into a small
prototype without touching a persistent production worker or weakening release
hygiene.

## Current Status

Historical checklist. A trusted-LAN endpoint MVP and helper scripts now exist
in this package, but this checklist remains useful for the remaining promotion
gates. Do not treat the endpoint MVP as production routing, tenant isolation,
or distributed cache correctness proof. Scoped no-MTP trusted-LAN suffix and
restore correctness passes are retained as public-safe summaries, while
MTP-enabled, restart, two-node, and logit/top-k validation remain separate
promotion gates.

## Phase Gates

| phase | entry gate | exit evidence |
|---|---|---|
| 0 capability audit | Existing source map reviewed against current upstream `llama.cpp` | Updated primitive classifications and no assumption that CachyLLama patches are required |
| 1 single-node local prototype | Isolated temporary server or tiny mechanics-only model approved | Cold versus restored correctness report with strict key mismatch controls |
| 2 two-node slot-file prototype | Two isolated compatible workers or process instances available | Node A save, Node B restore, checksum, logits/top-k/text comparison, and cleanup proof |
| 3 sidecar and registry | Blob lifecycle and registry schema reviewed | Local inventory, hydration, upload, sidecar-local eviction lease, registry lease design, residency, and eviction behavior captured |
| 4 router MVP | Worker/sidecar/registry mocks or safe isolated workers exist | OpenAI-compatible request routed with hit/miss/fallback decision log |
| 5 worker hooks | Upstream primitives proven insufficient | Minimal `/cache/*` hook proposal with patch size, ABI risk, and rollback path |
| 6 production hardening | MVP correctness and benchmark evidence reviewed | Tenant policy, encryption, audit log, GC, backpressure, failover, and RPO/RTO tested |

## Non-Negotiable Safety Gates

- Do not run against a persistent production worker.
- Do not start a second full Step 3.7 server without human approval and a fresh
  memory threshold pass.
- Do not track raw slot files, SSD cache directories, cache blobs, model files,
  private prompts, full logs, credentials, or raw workspaces.
- Do not use private repositories or private tenant data for router tests.
- Do not route cross-tenant cache by default.
- Do not publish manifests before content-address verification.
- Do not reuse a cache when any strict key field is missing or mismatched.
- Do not infer correctness from latency improvement.

## Required Interfaces Before Prototype

Worker-facing:

- `POST /cache/fingerprint`
- `POST /cache/restore`
- `POST /cache/commit`
- `GET /cache/stats`

Sidecar-facing:

- `GET /inventory`
- `POST /hydrate`
- `POST /upload`
- `POST /evict`
- `POST /leases/acquire`
- `POST /leases/release`
- `POST /verify`
- `GET /health`

Registry-facing:

- lookup compatible manifest;
- publish verified manifest;
- acquire/release registry lease;
- update residency;
- mark blob corrupt;
- expire/delete tenant scope;
- append audit event.

## Required Schemas

Initial schema contracts:

```text
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

Before any runtime prototype, confirm:

- event schema complete;
- validation-result schema complete;
- policy schema complete;
- mocked decision and validation traces reviewed;
- offline contract validator passes;
- offline replay harness emits valid decision events from synthetic request,
  worker, registry, and policy fixtures;
- replay input fixture validation passes for synthetic requests, workers,
  registry manifests, policies, positive schema fixtures, and mocked output;
- replay residency validation requires explicit local-copy status, and stale
  residency routes to durable hydration or cold prefill instead of a hot hit;
- mocked registry/ranking prototype golden cases pass for hot-local, durable,
  capacity fallback, policy denial, restore-validation quarantine, private
  disabled scope, and strict-mismatch decisions while emitting contract-valid
  decision events without runtime access;
- strict-key negative fixtures reject model, tokenizer, template, runtime ABI,
  backend, KV, context/checkpoint, flash attention, reasoning/template, MTP,
  token-prefix, scope, tenant, conversation, worker capability, and capacity
  mismatches before restore;
- negative privacy and fail-closed fixtures reject raw prompt, raw tenant, raw
  conversation, raw cache path, raw environment, secret-like strings,
  cross-tenant reuse, missing restore cache identity, failed restore without
  fallback, and corrupt restore without quarantine;
- future prototype emits these events before any integration with live workers.

## First Prototype Sequence

1. Reconfirm upstream slot save/restore support on the target source commit.
2. Select a tiny public prompt set and deterministic sampling.
3. Start an isolated temporary worker or mechanics-only model on a separate
   port.
4. Generate a worker fingerprint for the prompt prefix.
5. Cold prefill and save/commit a cache artifact.
6. Validate the artifact manifest against the strict schema.
7. Restore in a fresh compatible worker.
8. Compare next-token logits or top-k distribution.
9. Compare deterministic continuation text.
	10. Run mismatch controls for tenant, model, tokenizer, template, backend, KV,
	    context, checkpoint, and MTP fields.
	11. Record hit/miss/fallback decisions with basis fields.
	12. Clean up temporary processes and raw cache directories.

Use `docs/reference/cache-restore-correctness-plan.md` for the retained detailed
single-node restore procedure. Use
`docs/reference/second-node-cache-validation-plan.md` only after a second
equivalent Strix Halo host exists; synthetic Node A/Node B fixtures are not
multi-node evidence.

## Minimum Public Evidence Before Claims

| claim | required evidence |
|---|---|
| router can avoid cold prefill | restored TTFT plus processed-token/log evidence and cold control |
| restore is correct | logits/top-k or deterministic text match plus mismatch controls |
| multi-node reuse works | Node A save, durable upload, Node B hydrate/restore, and correctness comparison |
| tenant/scope miss-masking works | wrong-tenant lookup and conversation-scope mismatches are rejected before hydrate/generation, admin-audited, and client-masked as a scoped miss; stronger tenant isolation still needs auth-derived tenant mapping and live multi-tenant probes |
| durable blob store works | temp write, fsync, hash, upload, registry publish, verify, and GC evidence |
| sidecar eviction/GC is production-safe | sidecar-local lease denial, stale-residency hydrate or cold-prefill recovery, and GC/reference tests |
| production-ready routing exists | Phase 6 hardening evidence, not just MVP request success |

## Open Decisions

These require human or maintainer review before implementation:

- license terms for any reusable router code;
- prototype language and framework;
- Postgres and object-store deployment choice;
- encryption-at-rest mechanism and key ownership;
- tenant namespace policy;
- whether to support `global_system` cache in the first MVP;
- acceptable RPO/RTO targets;
- memory threshold for temporary workers on Strix Halo nodes;
- whether worker hooks should be upstream patches, a sidecar wrapper, or a
  local experimental patch stack.
