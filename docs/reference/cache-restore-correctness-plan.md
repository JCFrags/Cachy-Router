# Cache Restore Correctness Plan

/goal

This plan is current when a maintainer can review the first one-node cache
restore correctness campaign without touching the persistent `nimo-1:8081`
service, confusing synthetic router replay with KV correctness, or treating
latency improvement as proof of correct restore.

## Status

Plan only. No slot save/restore correctness result is recorded here. Do not run
this plan against the persistent `nimo-1:8081` service. Use an isolated temporary
worker, a tiny mechanics-only model, or an owner-approved maintenance window.

Current hardware boundary: only one AMD Strix Halo host is available. This plan
can prove same-host restore mechanics and negative compatibility behavior, but it
cannot prove cross-node distributed cache correctness.

## One-Node Versus Two-Node Boundary

| question | one `nimo-1` node can test now with approval | requires second Strix Halo node |
|---|---|---|
| slot/cache artifact can be saved and restored in an isolated process | yes | no |
| restored state matches cold prefill for next-token logits/top-k | yes | no |
| deterministic continuation matches after restore | yes | no |
| strict key mismatches fail closed | yes | no |
| MTP-enabled restore correctness on the same hardware/backend | yes, if isolated and approved | no |
| artifact survives process restart on the same node | yes | no |
| artifact is portable across identical nodes | no | yes |
| sidecar hydration from durable store to another worker | scripted stand-in only | yes |
| cross-node latency/TTFT improvement | no | yes |
| cache-aware router multi-node placement | offline fixtures only | yes |

## Claims Not Yet Proven

- cache blobs can be restored without full prefill;
- restored state produces the same next-token distribution as cold prefill;
- MTP/speculative decoding state is save/restore compatible;
- slot files are portable across workers, process lifetimes, or hosts;
- cache-aware routing can safely serve production traffic;
- cross-node restore works.

## Entry Gates

All gates must be true before a runtime test starts:

| gate | requirement |
|---|---|
| upstream capability audit | `docs/reference/upstream-cache-primitives-audit.md` refreshed for the target `llama.cpp` commit and flags |
| service isolation | test runs on a separate approved worker/port or tiny mechanics-only model; persistent `nimo-1:8081` is not restarted or reconfigured |
| memory threshold | owner records a numeric `MemAvailable` threshold and fresh reading above it for the planned run class |
| artifact policy | raw slot/cache blobs, full logs, and scratch directories are ignored and outside public release artifacts |
| prompt policy | prompts are synthetic/public and contain no private repositories, credentials, personal data, or proprietary code |
| cleanup plan | temporary process, slot files, and raw cache artifacts have explicit cleanup commands |
| stop criteria | stop on OOM risk, failed health check, unexpected service mutation, runaway logs, or private-data exposure |

## Minimum Metadata

Capture these fields in a small curated summary before interpreting results:

- `llama.cpp` source commit and binary path;
- build backend and driver lane;
- model ID, GGUF shard list, model hash, and tensor manifest hash;
- tokenizer hash and effective chat-template hash;
- context size, checkpoint config, KV types, flash attention mode, and MTP flags;
- prompt family name and prompt hash, not raw private prompt text;
- save/restore API or slot-file primitive used;
- raw artifact storage path policy and cleanup status;
- active context tokens and generated tokens for each request;
- TTFT, prompt processing speed, eval speed, restore latency, and fallback reason.

## Positive Test Matrix

| case | purpose | required comparison |
|---|---|---|
| cold baseline | establish no-cache next-token and timing baseline | deterministic request with no restored state |
| restore after system prompt | validate a short shared prefix restore point | next-token logits/top-k and deterministic text against cold prefill |
| restore mid-conversation | validate normal chat history restore | logits/top-k, generated text, and processed prompt-token delta |
| restore after tool output | validate template/tool boundary state | logits/top-k and deterministic continuation |
| branch/retry restore | validate shared-prefix branch reuse | cold branch request versus restored branch request |
| near-context-limit restore | validate boundary behavior without crossing active context limit | correctness plus memory/headroom notes |
| MTP disabled restore | isolate non-speculative restore correctness | same model/runtime with MTP off, if approved |
| MTP enabled restore | validate current Step 3.7 MTP lane | same strict key plus draft model/config fields |

## Negative Controls

Every mismatch must produce a miss, policy denial, fallback, or quarantine. No
negative case may restore.

| mismatch | expected behavior |
|---|---|
| wrong model hash or tensor manifest | cache miss |
| wrong tokenizer or chat template | cache miss |
| wrong `llama.cpp` commit or cache ABI | cache miss |
| wrong backend or driver lane | cache miss |
| wrong KV type, unified-KV mode, or flash attention mode | cache miss |
| incompatible context size or checkpoint config | cache miss |
| MTP enabled/disabled mismatch | cache miss |
| wrong draft model hash or draft config | cache miss |
| wrong token-prefix hash or token count | cache miss |
| wrong tenant or conversation scope | policy denial |
| corrupt, truncated, or missing blob | quarantine and cold-prefill fallback |
| stale residency | self-heal through hydrate or cold prefill |

## Procedure

1. Confirm Git state is clean and record the package commit.
2. Refresh the upstream cache primitive audit for the target binary/source.
3. Confirm service isolation and memory threshold approval.
4. Start only the approved isolated worker or tiny mechanics-only model.
5. Capture worker capabilities and strict key metadata.
6. Run the cold baseline for each prompt family.
7. Save a cache point through the audited primitive.
8. Verify blob size, checksum, manifest schema, and raw-artifact location.
9. Restore into a fresh compatible slot/session.
10. Compare next-token logits or top-k distribution.
11. Compare deterministic text under fixed sampling.
12. Run negative controls and confirm fail-closed behavior.
13. Record decision and validation events using the cache-router contracts.
14. Stop temporary processes and clean raw artifacts according to the plan.
15. Write only a small sanitized summary to tracked data.

## Pass Criteria

A restore case passes only when:

- cold and restored next-token logits or top-k distributions match within the
  documented tolerance;
- deterministic text matches, or any mismatch is explained and bounded;
- the restored request avoids full prefill by a direct metric or log basis;
- strict key fields match exactly;
- raw artifacts are not tracked or indexed;
- failed validations require fallback, and corrupt artifacts are quarantined.

## Output Artifacts

Track only:

- a small README summary;
- redacted request/worker/cache metadata;
- validation-result JSONL rows;
- decision-event JSONL rows;
- aggregate timing table;
- cleanup confirmation.

The curated result folder should include these specific reviewed fields:

| artifact | required content |
|---|---|
| manifest | strict cache key, model/runtime hashes, token-prefix hash, `n_tokens`, scope, tenant/conversation hashes, and blob metadata |
| blob checksum record | content hash, byte size, write/fsync/verify status, and redacted storage location |
| restore validation result | validation type, pass/fail/error status, tolerance, fallback requirement, quarantine recommendation |
| next-token comparison | logits or top-k distribution comparison against cold prefill with tolerance |
| deterministic text comparison | fixed-sampling restored continuation versus cold continuation |
| timing table | cold TTFT, restored TTFT, restore latency, prompt tokens, cached tokens, processed prompt tokens, eval speed |
| cleanup proof | temporary process stopped, raw blobs/logs absent from Git, ignored paths verified |

Do not track raw slot files, cache blobs, full logs, raw prompts, private
transcripts, model files, or temporary server homes.
