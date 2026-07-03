# Distributed Cache Architecture

/goal

This document is complete when a future implementer can explain the distributed
cache problem, identify proven versus hypothetical pieces, audit upstream and
fork code paths before implementation, define safe cache keys and correctness
checks, and run an MVP sequence without replacing the stable upstream RADV
service.

## Status

Hypothesis and architecture track only. Cachy Router now has scoped no-MTP
trusted-LAN deterministic restore evidence, but distributed runtime cache
changes still require MTP-enabled, restart, two-node, and logit/top-k validation
before they are treated as production-ready.

## Problem

Local Strix Halo testing has single-node append-heavy prompt-cache evidence with
upstream llama.cpp, including a request-level no-cache control. The unsolved
problem is branch, retry, restart, and multi-node reuse: large shared prefixes,
edited histories, static tool/system prompts, and repeated sessions can still
force expensive prefill unless the needed checkpoint is resident and compatible.

## Boundary

Worker:
Inference and model-specific cache correctness.

Sidecar:
Local inventory, hydration, upload, verification, eviction, and disk accounting.

Router:
OpenAI-compatible API, scheduling, cache lookup, tenant policy, and failover.

Registry/store:
Durable metadata and immutable cache blobs.

Out of scope for the MVP:

- replacing the stable upstream b9828 RADV service;
- claiming raw decode or prefill speed wins;
- multi-host scheduling before single-host SSD correctness is proven;
- sharing cache entries across incompatible model, runtime, tokenizer, template,
  tenant, or conversation states.

## Current Evidence

Evidence anchors:

- upstream `--cache-prompt` append-heavy reuse has short single-node evidence;
- branch reuse needs checkpoints in the current upstream b9828 evidence;
- CachyLLama is a current but invasive fork with SSD checkpoint/state paths;
- Cachy Router has scoped no-MTP trusted-LAN suffix and deterministic restore
  passes recorded as public-safe summaries;
- short runtime comparisons did not show a meaningful raw-speed advantage for
  CachyLLama;
- MTP-enabled, restart, two-node, and logit/top-k distributed correctness
  evidence remains required before broad cache-correctness claims.

References:

```text
docs/reference/upstream-cache-primitives-audit.md
docs/reference/cache-restore-correctness-plan.md
docs/reference/second-node-cache-validation-plan.md
evidence/cache-router-results-summary.md
```

## Evidence-Gated Approach

Every architecture step must name:

- claim being tested;
- minimum benchmark or official-suite evidence required;
- rollback path;
- artifact path for raw logs and summaries;
- cache invalidation behavior when validation fails.

No distributed-cache feature graduates from hypothesis until it passes smoke,
append, branch, restart, disk-accounting, and correctness checks.

## Upstream Capability Audit

Before porting or reimplementing anything, audit current upstream llama.cpp and
the CachyLLama fork for:

- `--cache-prompt`;
- `--cache-reuse`;
- `--ctx-checkpoints`;
- `--cache-ram`;
- `--cache-idle-slots` / `--no-cache-idle-slots`;
- `--slot-save-path`;
- slot save/restore APIs;
- router/model-management mode;
- recurrent-state checkpointing behavior;
- request routing fields such as `llama_user_id`;
- state serialization and restore APIs;
- eviction and cache namespace behavior.

Classify CachyLLama pieces as:

- already upstream;
- wrap externally;
- small patch;
- do not port.

Prefer extracting the smallest design or API lesson over adopting a fork as the
default runtime.

Use `docs/reference/upstream-cache-primitives-audit.md` as the retained
checklist for classifying each primitive and deciding whether it is already
upstream, externally wrappable, a small patch, an experimental lane, unsafe to
port, or still unknown.

Use `docs/architecture/cache-router.md` for the router-specific architecture:
OpenAI-compatible ingress, strict cache-key lookup, tenant policy, cache-aware
placement, sidecar hydration/upload, registry operations, and failure fallback.
Use `docs/architecture/cache-router-mvp-checklist.md` before moving that router
track from design into an isolated prototype.

Use `docs/reference/cache-restore-correctness-plan.md` for the retained one-node
restore correctness procedure, and
`docs/reference/second-node-cache-validation-plan.md` for the future two-node
validation plan. The second-node plan is not evidence that multi-node restore
works.

## Cache Key Fields

A cache key must include at least:

- model architecture, model hash, and GGUF tensor manifest hash;
- tokenizer hash, chat template hash, tools schema hash, system prompt hash,
  and special-token policy;
- llama.cpp source commit, cache ABI, and local patchset ID;
- backend/runtime lane and GPU driver lane;
- KV cache types, KV unification mode, context size, and checkpoint
  configuration;
- RoPE/YaRN scaling metadata;
- flash-attention mode, reasoning format, and Jinja/template mode;
- MTP/speculative enablement, draft model hash, and draft config;
- `n_parallel` and `n_seq_max` lane;
- token-prefix hash and token count;
- tenant namespace;
- conversation namespace.

Do not key only on raw prompt text.

## Correctness Tests

Minimum correctness sequence:

1. Cold prefill on node A.
2. Save checkpoint.
3. Restore on node B or an isolated process.
4. Compare logits or top-k distribution against a no-cache control.
5. Compare generated text under deterministic sampling.
6. Repeat for system prompt, mid-conversation, tool-output, branch/retry, and
   near-context-limit restore points.

A failed validation must force recompute, not partial reuse.

## Security And Tenancy

No cross-tenant reuse by default. Global reuse is only acceptable for
operator-controlled system/tool prompts with explicit opt-in.

Required controls:

- tenant namespace;
- conversation namespace;
- encryption at rest for durable blobs;
- access audit log;
- deletion and garbage-collection policy;
- secret-pattern checks before any cache metadata is published.

Registry records should avoid raw private prompt text when hashes and bounded
metadata are sufficient.

## Durability

Define RPO and RTO before production use. Treat cache files as acceleration data
that may be deleted, corrupted, partially written, or invalidated by version
changes.

Durable write sequence:

1. Write temp blob.
2. `fsync`.
3. Hash and verify.
4. Upload to store.
5. Commit registry transaction.
6. Publish cache entry.

Fallback must always be normal prefill.

## Registry Recommendation

Postgres should be the source of truth. Redis can be an optional hot lookup
cache. SQLite is appropriate only for node-local sidecar inventory.

Registry metadata:

- cache key;
- artifact path or content address;
- byte size;
- created and last-hit time;
- hit/miss counters;
- validation status;
- runtime/model compatibility;
- eviction priority;
- source benchmark or run id.

Do not store raw model files, private transcripts, or opaque unbounded blobs in
the registry.

## MVP Sequence

1. Preserve upstream b9828 RADV as the default service.
2. Audit upstream slot/cache primitives.
3. Classify CachyLLama patch areas.
4. Run any prototype on a separate service/port.
5. Prove smoke load with Step 3.7 and official smoke tests.
6. Prove append behavior does not regress against upstream lean profile.
7. Prove branch reuse against the existing branch probe.
8. Prove warm restart reuse from SSD.
9. Prove bounded disk growth and safe eviction.
10. Add sidecar plus registry metadata around upstream slot files.
11. Add minimal worker cache hooks only if required.
12. Evaluate multi-session or distributed lookup behavior.
