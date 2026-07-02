# Upstream Cache Primitives Audit

/goal

This audit checklist is complete when a maintainer can compare upstream
`llama.cpp` and CachyLLama cache primitives, classify each primitive, and decide
whether it should be used, wrapped, patched, isolated, or rejected before any
distributed-cache or SSD-cache implementation work begins.

## Status

Planning and source-audit track only. Do not port CachyLLama cache code, replace
the stable upstream RADV service, start a new persistent service, or run
SSD-backed cache experiments from this checklist alone.

The immediate purpose is to map current upstream capabilities against
CachyLLama behavior and identify the smallest evidence-gated next experiment.

## Read-Only Refresh 2026-06-30

This refresh used only read-only commands:

- noninteractive `ssh nimo-1` source/help/process inspection;
- local `git` and `rg` over external reference mirrors under
  `05-external-sources/sources/`;
- no service restart, kill, profile application, slot save/restore call, cache
  write, benchmark run, or model download.

Current upstream evidence:

| item | observed evidence | implication |
|---|---|---|
| running source commit | remote `nimo-1:/home/connorb/llama.cpp` is `ebd048fc5e4b43ec4e0b4abe0b9bf66e1724dad0`; the first status lines were clean | the source tree matches the documented b9828 lane for this audit snapshot |
| binary flag support | `llama-server --help` lists `--cache-prompt`, `--cache-reuse`, `--ctx-checkpoints`, `--cache-ram`, `--cache-idle-slots`, `--slot-save-path`, `--kv-unified`, `--spec-draft-model`, and `--spec-type ... draft-mtp` | the current upstream binary has the primitives needed for isolated wrapper/sidecar experiments |
| sequence state APIs | remote source contains `llama_state_seq_get_data_ext` / `llama_state_seq_set_data_ext` in `include/llama.h`, `src/llama-context.cpp`, `common/common.cpp`, `tools/server/server-context.cpp`, and `tools/server/server-task.cpp` | slot/checkpoint artifacts must key the exact runtime ABI, model state, KV types, and draft-state behavior |
| slot save/restore endpoint docs/tests | remote source contains `tools/server/README.md` sections for `POST /slots/{id_slot}?action=save` and `restore`, plus `tools/server/tests/unit/test_slot_save.py` | upstream slot save/restore is a real primitive, but correctness still requires isolated restore tests before use |
| persistent service configuration | read-only `/proc/<pid>/cmdline` filtering showed cache/MTP flags on the active service and did not show `--slot-save-path` | do not issue slot save/restore calls to the persistent `nimo-1:8081` service; use only a reviewed temporary server plan |

Reference-mirror evidence:

| source | commit | observed evidence | implication |
|---|---|---|---|
| CachyLLama mirror | `83af232aad85daa62175ba6220c1200f67089b8a` | `README.md`, `common/kv-ssd-cache.*`, `common/kv-ssd-system-cache.*`, `common/kv_page_manager.*`, and server files expose `--cache-ssd*`, global system-prompt cache, `llama_user_id`, and `--max-concurrent-per-user` paths | fork-specific SSD behavior remains an experimental lane; do not port by default |
| llama-ai wrapper mirror | `42306475c6a26aa363880d57696c82627e3039a3` | wrapper scripts and historical logs reference SSD flags and system-prompt cache behavior | useful provenance and runbook material, not package-shipped runtime proof |

Current conclusion: use upstream primitives first. CachyLLama SSD paths are
design inputs and future isolated experiments, not required patches for the
single-node MTP campaign or the first cache-aware router prototype.

## Delta Decision Table

Default decision: do not port CachyLLama behavior into the main Step 3.7 lane
until source evidence, isolated runtime correctness tests, and release hygiene
all pass. Prefer upstream primitives and external wrappers first.

| feature | upstream support status | CachyLLama / wrapper status | evidence source | action | runtime test needed |
|---|---|---|---|---|---|
| prompt-cache enable/reuse | present upstream via `--cache-prompt` and `--cache-reuse` | same lineage plus fork integration | remote b9828 help/source; `common/arg.cpp`; source map | already upstream | append/no-cache and branch/retry controls before guidance changes |
| context checkpoints and RAM cache | present upstream via `--ctx-checkpoints`, `--cache-ram`, idle-slot flags | SSD tiering hooks build on top of checkpoint ideas | remote b9828 help/source; CachyLLama `arg.cpp`; wrapper run scripts | already upstream; wrap externally for experiments | checkpoint count matrix, memory-pressure eviction, no-context-shift behavior |
| slot save/restore files/API | present upstream via `--slot-save-path` and `/slots/{id}?action=save|restore` docs/tests | fork keeps the endpoint lineage and adds SSD restore surfaces | upstream `tools/server/README.md`, `test_slot_save.py`; persistent service lacks `--slot-save-path` | wrap externally first | isolated save/restore correctness with logits/top-k/text and mismatch controls |
| sequence-state serialization | present upstream via `llama_state_seq_get_data_ext` / `set_data_ext` paths | fork uses related state APIs near SSD paths | upstream `include/llama.h`, `src/llama-context.cpp`, server context/task paths | already upstream, but ABI-keyed | same-node restore with exact cache ABI, KV, model, template, and MTP fields |
| SSD checkpoint persistence | no upstream SSD analog found in this pass | fork-specific `--cache-ssd*`, `kv-ssd-cache.*`, page manager, server SSD files | CachyLLama `common/kv-ssd-cache.*`, `server-context-ssd-cache.*`, `kv_page_manager.*`; wrapper `llama-run.sh` | experimental lane; do not port | separate-port bounded SSD-cache probe with cleanup and correctness controls |
| global system-prompt cache | no upstream SSD analog found in this pass | fork-specific global system prompt cache | CachyLLama `kv-ssd-system-cache.*`; wrapper README cache path notes | do not port by default; model as router allowlist first | tenant-safe `global_system` allowlist, no user-content reuse, deletion/GC test |
| user namespace / per-user cap | no upstream server field found in this pass | fork-specific `llama_user_id` and `--max-concurrent-per-user` | CachyLLama `server-chat.cpp`, `server-task.cpp`, user-isolation design; wrapper README | wrap externally in router policy | tenant/conversation scope denial and audit-log tests |
| SSD hit/miss/restore observability | partial upstream metrics/logs only | fork has SSD-specific log strings and wrapper log evidence | source map and external logs; not current runtime proof | unknown; possible small patch only after wrapper limits are proven | observability probe for hit, miss, restore, discard, full-prefill, and fallback basis |
| MTP/speculative restore compatibility | MTP flags are present upstream | fork behavior not proven for current Step 3.7 MTP lane | remote b9828 help/source; active service preflight | unknown; do not patch yet | paired MTP-enabled and MTP-disabled restore correctness tests |

## Inputs

Use these existing package artifacts before inspecting new source:

| artifact | role |
|---|---|
| `docs/architecture/distributed-cache.md` | architecture boundary, cache keys, correctness rules, MVP sequence |
| `docs/architecture/cache-primitives-source-map.md` | current read-only source map and initial primitive classifications |
| `docs/architecture/cache-behavior-probe-plan.md` | gated behavior procedures for upstream slot save/restore and CachyLLama SSD cache |
| `reports/2026-06-28-cachyllama-source-audit.md` | current CachyLLama fork audit and SSD-cache hypothesis |
| `reports/2026-06-28-branch-reuse-results.md` | upstream checkpoint branch-reuse evidence |
| `reports/2026-06-28-lean-cache-deep-run.md` | upstream append-cache evidence |
| `docs/service-profiles.md` | upstream lean and checkpoint profile intent |

## Primitives To Inspect

Upstream `llama.cpp`:

| primitive | audit question |
|---|---|
| `--cache-prompt` | What prompt-prefix reuse does upstream already provide for append-heavy traffic? |
| `--cache-reuse` | What semantics does reuse distance or similarity have in current server mode? |
| `--ctx-checkpoints` | What branch/retry reuse is available without SSD state? |
| `--cache-ram` | What RAM pool behavior and eviction policy are observable? |
| `--cache-idle-slots` / `--no-cache-idle-slots` | What state is retained for idle slots, and when is it discarded? |
| `--slot-save-path` | Can slot state be saved, restored, and externally managed without patching? |
| slot save/restore API | Is there a stable API for sidecar-controlled save/restore? |
| state serialization/restore | What compatibility keys are required for correctness? |
| server slot matching | Which prompt, cache, and conversation fields affect slot selection? |
| recurrent-state handling | Does the model family need extra state beyond KV cache? |
| router/model-management mode | Can routing remain external, or does the server need internal model routing? |
| metrics/log fields | Can hits, misses, processed tokens, discarded tokens, and restore events be proven? |
| eviction and namespace behavior | Can tenant/conversation isolation be enforced outside the server? |

CachyLLama:

| primitive | audit question |
|---|---|
| `--cache-ssd*` flags | Which behavior is unique to SSD persistence versus upstream RAM checkpoints? |
| `--max-concurrent-per-user` | Is this routing control relevant outside the fork? |
| `llama_user_id` | Can user namespace be modeled externally without changing the API? |
| `llama_state_seq_get_data_ext` / `llama_state_seq_set_data_ext` | Are these narrow APIs, invasive patches, or unsafe ABI dependencies? |
| SSD checkpoint load-before-prefill | Does it avoid prefill under controlled branch/restart tests? |
| global system prompt cache | Can it be made tenant-safe and opt-in only? |
| user-scoped checkpoint namespace | What key fields prevent cross-user reuse? |
| page manager | Is page accounting bounded, verifiable, and crash-safe? |
| tiering and prefetch | Does it improve branch/restart behavior without harming append behavior? |
| eviction | Does it preserve correctness under pressure and after deletion? |

Files called out by the existing source audit:

```text
05-external-sources/sources/CachyLLama/common/arg.cpp
05-external-sources/sources/CachyLLama/common/kv-ssd-cache.{h,cpp}
05-external-sources/sources/CachyLLama/common/kv-ssd-system-cache.{h,cpp}
05-external-sources/sources/CachyLLama/common/kv_page_manager.{h,cpp}
05-external-sources/sources/CachyLLama/tools/server/server-context.cpp
05-external-sources/sources/CachyLLama/tools/server/server-context-ssd-cache.{h,cpp}
05-external-sources/sources/CachyLLama/tools/server/server-chat.cpp
05-external-sources/sources/CachyLLama/tools/server/server-task.{h,cpp}
```

## Evidence To Capture

For each primitive, capture:

- upstream commit and CachyLLama commit;
- build flags and backend lane;
- exact launch command or source file/function path;
- model path summary, GGUF metadata, tokenizer hash, and chat-template hash when
  behavior depends on runtime state;
- KV cache types, flash-attention mode, context size, checkpoint flags, and cache
  flags;
- request trace: request fields, tokenization, slot/cache match, checkpoint
  restore/save, prefill/decode, and eviction;
- runtime proof: server logs, metrics before/after, slot state, prompt tokens,
  cached tokens, processed tokens, wall time, prompt tok/s, decode tok/s, and
  cache hit/miss reason;
- disk proof for SSD paths: cache directory manifest, file count, largest file,
  `du`, content hashes, and bounded-growth result;
- controls: no-cache request, cache-cleared run, older-prefix branch, restart
  run, and isolated-process restore.

Do not store raw private prompts, raw Hermes homes, credentials, `.env` files,
model files, checkpoints, or unbounded raw logs in Git.

## Audit Worksheet

Use this table shape when producing the first read-only source mapping:

| upstream primitive | CachyLLama analog | source path / commit | evidence command or artifact | classification | follow-up test |
|---|---|---|---|---|---|
| `--cache-prompt` / `--cache-reuse` | SSD prefix and global system-prompt cache paths | upstream `common/arg.cpp`, `tools/server/README.md`; CachyLLama `README.md`, `tools/server/server-context.cpp`; commits `ebd048fc5...` and `83af232...` | remote help/source `rg`; `docs/architecture/cache-primitives-source-map.md` | upstream prompt cache is `already-upstream`; CachyLLama SSD/global cache is `experimental-lane` | append/no-cache control, older-prefix branch, tenant-safe global-system allowlist test |
| `--ctx-checkpoints`, `--cache-ram`, idle-slot cache | `--cache-ssd-checkpoints`, hot/warm/cold tiers | upstream `common/arg.cpp`, `tools/server/server-context.cpp`, `tools/server/README.md`; CachyLLama SSD/page-manager files | remote help/source `rg`; mirror source search | upstream checkpoints/RAM cache are `already-upstream`; fork SSD tiers are `experimental-lane` | branch/retry checkpoint comparison, memory-pressure eviction evidence |
| `--slot-save-path` or slot save API | SSD checkpoint serialize/restore APIs | upstream `tools/server/README.md`, `tools/server/tests/unit/test_slot_save.py`, server-context/task sequence-state calls; CachyLLama `server-context-ssd-cache.*` | remote source `rg`; persistent cmdline filter did not show `--slot-save-path` | upstream slot files are `external-wrapper` candidates; CachyLLama SSD restore remains `experimental-lane` | isolated temporary-server restore correctness with deterministic continuation and mismatch controls |
| upstream slot matching and request policy | `llama_user_id` and user-scoped namespace | CachyLLama `tools/server/server-chat.cpp`, `tools/server/server-task.cpp`, and the external mirror's user-isolation design note | mirror source search | `external-wrapper`; keep tenant/conversation policy in router unless runtime evidence proves a worker hook is necessary | tenant/conversation mismatch and cross-tenant denial tests |
| MTP/speculative state | draft-model state save/restore paths | upstream `common/speculative.cpp`, `tools/server/server-context.cpp`, `tools/server/server-task.cpp` | remote help/source `rg` for `draft-mtp` and sequence-state calls | MTP flag support is `already-upstream`; MTP restore correctness is `unknown` | paired MTP and non-MTP restore correctness tests before cache-router runtime use |
| upstream metrics/logs | SSD hit/miss/restore logging | upstream metrics/log fields are partial; CachyLLama has SSD hit/restore log strings near server SSD paths | source map and future bounded log capture | `unknown`; possible `small-patch` only if wrapper cannot observe hit/miss/fallback safely | hit/miss/discard/restore/full-prefill observability check |

Leave a cell blank rather than inferring an answer. A primitive remains
`unknown` until both source evidence and a follow-up test are identified.

## Classification

| classification | meaning |
|---|---|
| `already-upstream` | Source primitive exists upstream; no port is assumed unless behavior gates pass |
| `external-wrapper` | Candidate for sidecar/router wrapping after behavior, correctness, and safety gates pass |
| `small-patch` | Narrow upstreamable API or metric gap with low runtime risk |
| `experimental-lane` | CachyLLama-only or invasive behavior requiring a separate service and port |
| `do-not-port` | Weakens correctness, tenancy, reproducibility, or stable RADV service safety |
| `unknown` | Insufficient source or runtime evidence; must not graduate |

Every primitive must have one classification plus the evidence path supporting
that classification.

## Correctness Tests

Minimum sequence:

1. Cold prefill with no cache control.
2. Save checkpoint or cache artifact.
3. Restore in an isolated process or separate node.
4. Compare logits or top-k distribution against the no-cache control.
5. Compare deterministic generated text with fixed seed and identical sampling.
6. Repeat across append, branch/retry, older-prefix branch, system prompt reuse,
   mid-conversation, tool-output, near-context-limit, restart, and cache-eviction
   cases.
7. Validate key mismatch behavior for tokenizer, template, KV type, commit,
   model hash, tenant, conversation namespace, context size, and checkpoint
   config.

Failure rule: any mismatch forces full recompute. Never allow partial reuse after
a correctness mismatch.

## Stop Conditions

Stop the audit or experiment if:

- the stable upstream RADV service would be replaced or disturbed;
- cache files grow without a configured bound;
- restore corrupts state or correctness comparisons diverge;
- metrics cannot prove reuse, miss, discard, or restore behavior;
- cross-tenant or global reuse is possible without explicit namespace and opt-in
  controls;
- raw private prompt text or unsafe paths would enter tracked artifacts;
- CachyLLama shows no branch, restart, or static-prefix advantage over upstream
  checkpoints under controlled tests.

## Next Allowed Step

The read-only source map and this audit refresh both point to the same sequence:
keep using upstream primitives first, then run an isolated reduced-context
upstream slot save/restore correctness probe only after the temporary-server
safety gates in `docs/architecture/cache-behavior-probe-plan.md` pass.

The next safe documentation/script action is to add a cache-primitive result
validator or fixture, not to port CachyLLama code or touch `nimo-1:8081`.

Do not run a side-by-side SSD-cache service until the source map and behavior
plan have been reviewed and the current MTP benchmark lane is stable enough to
avoid interference.
