# Two-Node Cache Router Autonomous Plan

/goal

This plan is current when it gives a maintainer or autonomous agent enough
specific work to spend an eight-hour implementation session turning the current
one-node cache-router proof into the first cross-worker Strix Halo
cache-router validation, without weakening evidence quality, privacy, or
publication readiness.

The current lab has two worker PCs, `nimo-1` and `nimo-2`. That is the first
validation topology, not an architectural limit. The router should load an
arbitrary worker inventory and schedule across N compatible workers.

## Objective

Build a LAN-usable OpenAI-compatible cache router that can route one
Step 3.7 Flash workload across the first two compatible workers, `nimo-1` and
`nimo-2`, preserve reusable prefix KV/slot cache in a router-owned durable
store, hydrate worker-local hot cache on the selected node, and prove that a
continuation on `nimo-2` after cache hydration is within an expected margin of
native hot continuation on `nimo-1`.

The required proof is not just "a cache file can be copied." It must show:

- `nimo-1` and `nimo-2` run compatible model/runtime/cache profiles;
- a prefix processed and saved from `nimo-1` is stored in the router durable
  store with a strict compatibility manifest;
- `nimo-2` starts without the worker-local slot file, receives/hydrates the
  durable blob, restores it, and serves only the suffix/continuation;
- restored `nimo-2` continuation prompt-processing wall time is in the expected
  range for native restored continuation on `nimo-1`, not in the cold full
  prompt-processing range;
- all evidence is captured as small sanitized summaries, not raw prompts,
  raw logs, model files, slot blobs, credentials, or private repositories.

## Current Read-Only Snapshot

Captured on 2026-07-01 before this plan was written. A post-outage read-only
preflight is tracked at
`data/cache_router_poc/2026-07-01-two-node-preflight/README.md`.

| field | `nimo-1` | `nimo-2` | parity state |
|---|---|---|---|
| LAN IP | `10.0.0.189` | `10.0.0.21` | usable |
| kernel | `7.1.1-2-cachyos` | `7.1.2-3-cachyos` | close, not identical |
| active runtime | router-managed worker on `127.0.0.1:18082` | live service on `0.0.0.0:8081` | not identical |
| active context | `65536` | `256000` | mismatch |
| draft depth | `--spec-draft-n-max 2` | `--spec-draft-n-max 3` | mismatch |
| runtime path | `/home/connorb/llama.cpp/build-vulkan-radv-b9828/bin/llama-server` | `/home/connorb/llama.cpp/build/bin/llama-server` | mismatch |
| model path | `/home/connorb/models/Step-3.7-Flash-GGUF/Q4_K_S/...00001...gguf` | `/home/connorb/models/Step3.7/Q4_K_S/...00001...gguf` | path mismatch; file sizes match |
| MTP path | `/home/connorb/models/Step-3.7-Flash-GGUF/Step3.7-flash-mtp-Q8_0.gguf` | same path | likely match |
| memory available | about `12 GiB` | about `6 GiB` | tight on `nimo-2` |
| legacy service | `8081` inactive | `8081` active | mismatch |

The first implementation milestone is therefore not cross-node cache transfer.
It is a controlled service-window decision to create a matched router-managed
worker profile on both hosts.

Update: the daemon now has a worker-inventory seam and can parse an arbitrary
`workers` list, but the current live stack still starts one router-managed
worker by default. The remaining implementation work is to run matched workers
on `nimo-1` and `nimo-2`, schedule across that inventory, then run the
cross-worker hydration/correctness/timing proof.

The router is intended to be independent of the worker nodes. It can be
colocated with `nimo-1` for early testing, but the durable store, OpenAI
ingress, scheduling, and registry should also run on a third non-worker PC. The
one-node MVP's direct local filesystem copy is therefore only a colocated
prototype path; two-node work needs an explicit worker/sidecar transport for
worker-local NVMe hydration.

## Product Spec

### User-Facing Endpoint

- Router binds on `nimo-1` for the first two-node MVP.
- LAN base URL: `http://10.0.0.189:18080/v1`.
- Model name: `Step-3.7`.
- API key / Authorization: not required for trusted home-LAN mode.
- Required public endpoints:
  - `GET /health`;
  - `GET /v1/models`;
  - `POST /v1/completions`;
  - `POST /v1/chat/completions` pass-through;
  - `GET /router/status`;
  - `GET /router/workers`;
  - `GET /router/cache`;
  - `GET /router/decisions`;
  - `POST /router/cache/build`;
  - `POST /router/cache/use`;
  - `POST /router/cache/validate`;
  - `POST /router/admin/workers/{worker_id}/restart` only if an explicit
    service-window flag is supplied.

### Runtime Topology

```text
OpenAI client / Hermes
  -> nimo-1:18080 cache-router daemon
  -> router registry and durable blob store on nimo-1
  -> worker nimo-1:18082 with local slot-save path
  -> worker nimo-2:18082 with local slot-save path
```

The router may initially run on `nimo-1`; later it can move to a separate router
PC without changing the worker/sidecar contract. The implementation should
represent workers as a list, for example:

```json
[
  {"worker_id": "nimo-1", "worker_url": "http://10.0.0.189:18082"},
  {"worker_id": "nimo-2", "worker_url": "http://10.0.0.21:18082"}
]
```

Adding a third worker should be a config change plus parity validation, not a
code change.

### Worker Contract

Each worker is an independently restartable llama.cpp server with:

- matching model bytes;
- matching MTP draft bytes;
- matching llama.cpp commit/cache ABI or explicitly compatible ABI;
- matching backend family and compatible RADV/Vulkan driver state;
- matching context, KV, flash-attn, cache, speculative decoding, reasoning, and
  template flags;
- `--metrics`, `--slots`, and `--slot-save-path`;
- worker-local slot directory on local NVMe;
- sidecar-compatible file layout for hydration and verification.

### Sidecar Contract

The first implementation can put sidecar behavior inside the router stack
manager, but the code boundaries should already separate:

- worker process lifecycle;
- worker-local slot inventory;
- durable blob hydration;
- checksum verification;
- local residency updates;
- eviction and isolation of stale local slots.

### Durable Store Contract

Router-owned store layout:

```text
router-cache-root/
  router-store/
    blobs/sha256/<sha256-prefix>/<sha256>.slot
    manifests/<cache_key_hash>.json
    registry.json
    decisions.jsonl
  workers/
    nimo-1/
      slots/
      logs/
      inventory.json
    nimo-2/
      slots/
      logs/
      inventory.json
```

Do not track this directory in Git.

### Strict Cache Key

A restore is allowed only if all available strict fields match. Unknown fields
must produce a miss until a compatibility audit proves they are irrelevant.

Minimum fields:

- `cache_key_hash`;
- `token_prefix_hash`;
- `saved_token_count`;
- `slot_file_sha256`;
- `slot_file_size_bytes`;
- `source_worker_id`;
- `model_id`;
- `model_architecture`;
- `main_model_shard_paths`;
- `main_model_shard_sizes`;
- `main_model_shard_sha256` when available or required for publication run;
- `gguf_tensor_manifest_hash`;
- `tokenizer_hash`;
- `chat_template_effective_hash`;
- `special_token_policy`;
- `llama_cpp_source_commit`;
- `llama_cpp_version`;
- `llama_cpp_cache_abi_version`;
- `build_backend`;
- `vulkan_device_name`;
- `vulkan_driver_version`;
- `ctx_size`;
- `batch_size`;
- `ubatch_size`;
- `flash_attention_mode`;
- `kv_unified_mode`;
- `cache_type_k`;
- `cache_type_v`;
- `cache_ram`;
- `cache_reuse`;
- `ctx_checkpoints_config`;
- `parallel`;
- `no_context_shift`;
- `reasoning_format`;
- `jinja_template_mode`;
- `mtp_enabled`;
- `spec_draft_model_path`;
- `spec_draft_model_size`;
- `spec_draft_model_sha256`;
- `spec_draft_n_max`;
- `spec_draft_config`;
- `scope`;
- `tenant_hash`;
- `conversation_hash`;
- `cache_created_at`;
- `router_version`.

### Routing Rules

1. `bypass`: proxy to selected worker, no cache action.
2. `build`: process prefix on selected worker, save slot, ingest durable blob,
   publish manifest, and mark local residency.
3. `use`: find compatible manifest, prefer hot local residency on any healthy
   eligible worker, otherwise hydrate durable blob into the selected worker and
   restore before suffix-only request.
4. `auto`: use if compatible cache exists; otherwise build then use.
5. `refresh`: rebuild cache entry and supersede prior manifest.
6. `validate`: run restore correctness checks without publishing a performance
   claim.

Full-prompt replay must remain a diagnostic path. It is not the success path
unless llama.cpp proves it can skip the restored prefix when the full prompt is
resent.

## Success Criteria

### Functional Success

- Router serves normal OpenAI requests through either worker.
- Router can build a cache on `nimo-1`.
- Router can hydrate and restore the same cache on `nimo-2`.
- Router can continue using suffix-only routing on `nimo-2`.
- Router records decision events for miss, build, ingest, hydrate, restore,
  generate, fallback, validation, and completion.
- Router remains healthy after worker restarts.
- Durable registry survives router restart.

### Timing Success

For a synthetic public prefix in the 25k-35k token range:

- cold full prompt on `nimo-1`: measured as `cold_nimo1_prompt_ms`;
- native hot restored suffix on `nimo-1`: measured as
  `hot_nimo1_restore_plus_suffix_ms`;
- hydrated restored suffix on `nimo-2`: measured as
  `hydrated_nimo2_restore_plus_suffix_ms`;
- restored `nimo-2` continuation must be closer to native hot continuation than
  cold full prompt, with both:
  - `hydrated_nimo2_restore_plus_suffix_ms <= cold_nimo1_prompt_ms * 0.10`;
  - `hydrated_nimo2_restore_plus_suffix_ms <= hot_nimo1_restore_plus_suffix_ms
    * 1.25 + 500 ms`.

If those thresholds are not met, the run is still valuable, but the result is
`diagnostic_failure` or `partial_success`, not a router success.

### Correctness Success

Before publication-quality claims:

- deterministic suffix generation from cold restored state and hydrated restored
  state must match under controlled settings, or mismatch must be explained;
- next-token logits/top-k comparison must be captured when the endpoint can
  expose it or a compatible CLI harness can compute it;
- wrong model, wrong MTP depth, wrong context, wrong KV type, wrong tokenizer,
  wrong template, wrong tenant, corrupt blob, truncated blob, missing local file,
  and stale residency tests must all fail closed.

## Eight-Hour Autonomous Work Plan

### Hour 0-1: Integration And Safety Baseline

- Commit current `nimo-2` replication notes and refreshed release metadata.
- Create this plan, update roadmap/status/decision log, and commit.
- Confirm no unrelated dirty files except known operator work.
- Read-only SSH parity snapshot for both hosts.
- Confirm both hosts have enough disk for temporary slot blobs.
- Confirm both hosts have enough memory headroom before any worker restart.
- Decide exact matched worker profile for the first two-node run.
- Write rollback commands for both hosts before starting services.
- Confirm no private prompts, raw transcripts, credentials, or model files will
  enter the repo.
- Confirm release index excludes router cache roots, `*.slot`, raw logs, raw
  prompts, and remote worker directories.

### Hour 1-2: Worker Profile Parity And Stack Manager Design

- Extend the stack manager so workers are data-driven:
  `workers: [{id, host, bind, port, slot_dir, log_dir, model_path, mtp_path,
  flags}]`.
- Add read-only `status` for each worker host.
- Add `preflight` that compares model sizes, hashes, runtime versions, flags,
  memory, disk, kernel, Vulkan device, and service state.
- Add `plan`/dry-run output that prints exact start/stop commands before
  mutation.
- Preserve the existing one-node LAN endpoint mode.
- Add explicit service-window guard for stopping existing `8081` services.
- Add worker-specific PID files.
- Add worker-specific log paths.
- Add worker-specific slot paths.
- Add remote file copy abstraction for durable blob hydration.
- Add failure recovery that restores or reports each worker independently.

### Hour 2-3: Router Multi-Worker Core

- Add worker registry to `cache_router_daemon.py`.
- Add `/router/workers`.
- Add `/router/decisions`.
- Add worker health cache with freshness timestamps.
- Add route selection that prefers hot local cache.
- Add durable hydrate path to a selected remote worker.
- Add local worker inventory refresh.
- Add manifest residency updates per worker.
- Add stale residency detection.
- Add restore failure fallback policy.
- Add cache miss/cold prefill policy.
- Add structured router decision events for each stage.

### Hour 3-4: Cross-Node Build/Use Procedure

- Add a controlled two-node POC command.
- Build prefix cache on `nimo-1`.
- Ingest durable blob in router store.
- Start or select fresh matched worker on `nimo-2`.
- Prove `nimo-2` worker-local slot is absent before hydration.
- Hydrate blob to `nimo-2`.
- Verify hydrated SHA256 and size.
- Restore on `nimo-2`.
- Send suffix only to `nimo-2`.
- Record prompt-processing timings.
- Run native hot continuation on `nimo-1`.
- Compare `nimo-2` continuation against native hot margin.

### Hour 4-5: Negative And Correctness Tests

- Add no-restore-before-hydration negative.
- Add wrong model path negative.
- Add wrong model hash negative.
- Add wrong MTP depth negative.
- Add wrong spec draft model negative.
- Add wrong context negative.
- Add wrong KV type negative.
- Add wrong tokenizer/template negative when observable.
- Add corrupt blob negative.
- Add truncated blob negative.
- Add stale registry residency negative.
- Add missing local slot negative.
- Add wrong tenant/scope negative.
- Add restart-router survival test.
- Add restart-worker survival test.
- Add deterministic generation comparison.

### Hour 5-6: OpenAI/Hermes Usability

- Preserve no-auth LAN lab mode unless owner asks otherwise.
- Verify `/v1/models`.
- Verify normal `/v1/chat/completions`.
- Verify normal `/v1/completions`.
- Verify cache extension through `/v1/completions`.
- Add explicit cache extension examples.
- Add Hermes Agent setup note.
- Add limitation that automatic system-prompt reuse requires request
  normalization/prefix recognition, not just a durable blob.
- Add optional cache-control request fields for `cache_id`, `scope`,
  `conversation_id`, `fallback_policy`, and `preferred_worker`.
- Add metrics in response metadata for cache hit/miss and selected worker.

### Hour 6-7: Evidence Capture And Reporting

- Create `data/cache_router_poc/YYYY-MM-DD-two-node-router/`.
- Write sanitized `README.md`.
- Write small `results.json`.
- Write `cache-router-events.jsonl`.
- Write redacted service snapshots for both hosts.
- Do not store raw prompts, slot files, raw logs, remote cache roots, or model
  files.
- Compute reduction tables.
- Add caveats for suffix-only semantics and correctness status.
- Update benchmark claim map as exploratory/two-node POC only.
- Update experiment status.

### Hour 7-8: Publication Hardening

- Run syntax check.
- Run cache-router contract validator.
- Run offline prototype validator.
- Run official non-live regression.
- Rebuild benchmark database if evidence was added through supported source
  paths.
- Rebuild release index.
- Run release audit.
- Run unsafe indexed-path scan.
- Remove `__pycache__` and `*.pyc`.
- Commit coherent patches.
- Prepare final report with exact commands, PIDs, ports, evidence paths,
  measurements, pass/fail status, and remaining blockers.

## Task Queue

Status values: `planned`, `allowed`, `gated`, `done`, `blocked`.

| # | status | task | lane | acceptance |
|---:|---|---|---|---|
| 1 | done | Preserve and commit `nimo-2` replication docs | repo | `nimo-2` notes remain tracked |
| 2 | done | Refresh release index after `nimo-2` docs | repo | audit release-index check passes |
| 3 | done | Remove generated Python caches before audit | repo | no `__pycache__` failure |
| 4 | planned | Add this two-node autonomous plan | planning | tracked doc exists |
| 5 | planned | Update roadmap hardware boundary from one-node to two-node available | planning | roadmap no longer says only one host |
| 6 | planned | Update experiment status for two-node cache-router lane | planning | lane marked planned/gated |
| 7 | planned | Add decision: two-node cache-router work is now allowed after parity gate | planning | decision log updated |
| 8 | allowed | Read-only SSH connectivity check for `nimo-1` | preflight | hostname/user/kernel captured |
| 9 | allowed | Read-only SSH connectivity check for `nimo-2` | preflight | hostname/user/kernel captured |
| 10 | allowed | Capture LAN IPs for both hosts | preflight | IPs recorded |
| 11 | allowed | Capture active llama-server process on `nimo-1` | preflight | cmdline redacted |
| 12 | allowed | Capture active llama-server process on `nimo-2` | preflight | cmdline redacted |
| 12a | done | Add read-only two-node parity preflight script | preflight | `scripts/cache_router_two_node_preflight.py`; post-outage result says not ready |
| 13 | allowed | Compare runtime binary paths | parity | mismatch reported |
| 14 | allowed | Compare llama.cpp versions | parity | version table |
| 15 | allowed | Compare kernel versions | parity | version table |
| 16 | allowed | Compare Vulkan device names | parity | device table |
| 17 | allowed | Compare RADV/Vulkan driver strings | parity | driver table |
| 18 | allowed | Compare main model shard sizes | parity | all shards match |
| 19 | allowed | Hash main model shards on both hosts | parity | hashes match or mismatch blocks restore |
| 20 | allowed | Compare MTP model sizes | parity | sizes match |
| 21 | allowed | Hash MTP model on both hosts | parity | hashes match |
| 22 | allowed | Compare active model paths | parity | path differences captured |
| 23 | allowed | Compare active context sizes | parity | mismatch blocks first test |
| 24 | allowed | Compare `batch` and `ubatch` | parity | values match |
| 25 | allowed | Compare KV cache types | parity | `q8_0/q8_0` match |
| 26 | allowed | Compare `--kv-unified` | parity | match |
| 27 | allowed | Compare `--cache-ram` | parity | match |
| 28 | allowed | Compare `--cache-reuse` | parity | match |
| 29 | allowed | Compare `--ctx-checkpoints` | parity | match |
| 30 | allowed | Compare `--no-context-shift` | parity | match |
| 31 | allowed | Compare MTP enablement | parity | match |
| 32 | allowed | Compare `spec-draft-n-max` | parity | mismatch blocks test |
| 33 | allowed | Compare reasoning format | parity | match |
| 34 | allowed | Compare Jinja/template flags | parity | match |
| 35 | allowed | Compare slot-save-path availability | parity | both router workers need it |
| 36 | allowed | Compare metrics/slots endpoint support | parity | both support |
| 37 | allowed | Check disk free for router store | safety | enough for slot blobs |
| 38 | allowed | Check disk free for each worker slot dir | safety | enough for slot blobs |
| 39 | allowed | Check memory headroom before service window | safety | threshold met or block |
| 40 | planned | Define matched POC profile | runtime | one profile applies to both hosts |
| 41 | planned | Decide first context size | runtime | likely `65536` for safety |
| 42 | planned | Decide first draft depth | runtime | choose 2 or 3, not mixed |
| 43 | planned | Decide router host | runtime | initial router on `nimo-1` |
| 44 | planned | Decide worker ports | runtime | no collision |
| 45 | planned | Decide rollback commands | runtime | commands recorded |
| 46 | planned | Add multi-worker config format | implementation | config parses |
| 47 | planned | Add worker config schema | implementation | standard-library validation |
| 48 | planned | Add `status` for multiple workers | implementation | both workers reported |
| 49 | planned | Add `preflight` command | implementation | parity JSON emitted |
| 50 | planned | Add `dry-run` command | implementation | no mutation |
| 51 | planned | Add worker PID tracking per host | implementation | no pid collision |
| 52 | planned | Add router PID tracking | implementation | survives restarts |
| 53 | planned | Add remote file copy helper | implementation | checksum verified |
| 54 | planned | Add worker-local inventory helper | implementation | slot presence/hashes reported |
| 55 | planned | Add durable blob layout helper | implementation | content-addressed path |
| 56 | planned | Add manifest writer | implementation | strict fields included |
| 57 | planned | Add manifest reader | implementation | validates required fields |
| 58 | planned | Add registry lock | implementation | no corrupt registry on concurrent write |
| 59 | planned | Add decision event append | implementation | JSONL contract-valid |
| 60 | planned | Add `/router/workers` | endpoint | reports both hosts |
| 61 | planned | Add `/router/decisions` | endpoint | recent decision events |
| 62 | planned | Add `/router/cache` worker residency view | endpoint | redacted paths |
| 63 | planned | Add worker selection policy | routing | hot local > hydrate > cold |
| 64 | planned | Add preferred-worker override | routing | operator can target node |
| 65 | planned | Add capacity guard | routing | unhealthy/full worker excluded |
| 66 | planned | Add policy denial path | routing | no unsafe restore |
| 67 | planned | Add fallback policy | routing | error vs cold fallback explicit |
| 68 | planned | Add restore failure quarantine | routing | corrupt blob marked suspect |
| 69 | planned | Add stale residency self-heal | routing | hydrate if local missing |
| 70 | planned | Add no-cache private-disabled path | routing | no persistence |
| 71 | planned | Build cache on `nimo-1` | runtime test | prefix saved and ingested |
| 72 | planned | Verify durable blob SHA256 | runtime test | hash match |
| 73 | planned | Verify manifest strict key | runtime test | all critical fields set |
| 74 | planned | Use cache hot on `nimo-1` | runtime test | native hot timing captured |
| 75 | planned | Isolate `nimo-2` local slot dir | runtime test | no accidental local hit |
| 76 | planned | Hydrate durable blob to `nimo-2` | runtime test | hash match |
| 77 | planned | Restore on `nimo-2` | runtime test | `n_restored` near saved tokens |
| 78 | planned | Continue suffix on `nimo-2` | runtime test | suffix-only prompt tokens |
| 79 | planned | Compare `nimo-2` restored vs cold | runtime test | <= 10% cold |
| 80 | planned | Compare `nimo-2` restored vs `nimo-1` hot | runtime test | within margin |
| 81 | planned | Use cache hot on `nimo-2` second time | runtime test | no hydration needed |
| 82 | planned | Restart router and use same cache | survival | cache survives router restart |
| 83 | planned | Restart `nimo-1` worker and use cache | survival | local or hydrated restore |
| 84 | planned | Restart `nimo-2` worker and use cache | survival | hydrated restore |
| 85 | planned | Negative: restore before hydration on `nimo-2` | negative | fails closed |
| 86 | planned | Negative: remove local slot after registry says resident | negative | hydrates or misses |
| 87 | planned | Negative: corrupt durable blob copy | negative | quarantine |
| 88 | planned | Negative: truncate durable blob copy | negative | reject |
| 89 | planned | Negative: wrong model hash | negative | miss |
| 90 | planned | Negative: wrong MTP hash | negative | miss |
| 91 | planned | Negative: wrong draft depth | negative | miss |
| 92 | planned | Negative: wrong context | negative | miss |
| 93 | planned | Negative: wrong KV type | negative | miss |
| 94 | planned | Negative: wrong template hash | negative | miss |
| 95 | planned | Negative: wrong tenant | negative | deny |
| 96 | planned | Negative: private-disabled scope | negative | no commit |
| 97 | planned | Deterministic generation compare cold vs restored | correctness | match or caveat |
| 98 | planned | Top-k/logit compare plan | correctness | implemented if API available |
| 99 | planned | Track mismatch diagnostics | correctness | useful failure report |
| 100 | planned | OpenAI normal chat test | endpoint | HTTP 200 |
| 101 | planned | OpenAI normal completion test | endpoint | HTTP 200 |
| 102 | planned | OpenAI cache build extension test | endpoint | cache built |
| 103 | planned | OpenAI cache use extension test | endpoint | cache used |
| 104 | planned | Hermes base URL smoke | endpoint | client can connect |
| 105 | planned | Streaming bypass check | endpoint | supported or clear error |
| 106 | planned | Streaming cached hit guard | endpoint | no unsafe partial stream |
| 107 | planned | Capture results README | evidence | sanitized |
| 108 | planned | Capture results JSON | evidence | small |
| 109 | planned | Capture decision JSONL | evidence | contract-valid |
| 110 | planned | Capture redacted service snapshots | evidence | no secrets |
| 111 | planned | Add benchmark caveat table | docs | no overclaim |
| 112 | planned | Update claim map | docs | exploratory two-node POC |
| 113 | planned | Update experiment status | docs | evidence path linked |
| 114 | planned | Update guide endpoint instructions | docs | Hermes ready |
| 115 | planned | Update release blockers | docs | remaining production blockers |
| 116 | planned | Update roadmap queue | docs | next state accurate |
| 117 | planned | Add raw evidence retention guard for new cache roots | hygiene | ignored/excluded |
| 118 | planned | Add release audit guard for `*.slot` | hygiene | no slot blobs in release |
| 119 | planned | Add release index descriptions for new scripts | hygiene | artifact map readable |
| 120 | planned | Run syntax check | verification | pass |
| 121 | planned | Run cache-router contract validator | verification | pass |
| 122 | planned | Run offline prototype validator | verification | pass |
| 123 | planned | Run official non-live regression | verification | pass/skips explained |
| 124 | planned | Rebuild benchmark DB if supported evidence added | verification | counts documented |
| 125 | planned | Rebuild release index | verification | current count |
| 126 | planned | Run release audit | verification | 10 pass / known dist warn |
| 127 | planned | Run unsafe indexed-path scan | verification | zero hits |
| 128 | planned | Remove Python caches | verification | clean audit |
| 129 | planned | Commit implementation patches in small units | git | reviewable commits |
| 130 | planned | Final owner report | handoff | commands, metrics, blockers |

## Test Matrix

| id | test | workers | expected result |
|---|---|---|---|
| T001 | read-only parity snapshot | both | all required fields captured |
| T002 | model shard hash parity | both | hashes identical |
| T003 | MTP hash parity | both | hashes identical |
| T004 | runtime version parity | both | identical or compatibility waiver |
| T005 | profile flag parity | both | identical first-run profile |
| T006 | cold full prompt on `nimo-1` | `nimo-1` | baseline prompt ms |
| T007 | build/save/ingest on `nimo-1` | `nimo-1` | durable blob published |
| T008 | hot restored suffix on `nimo-1` | `nimo-1` | native hot timing |
| T009 | restore-before-hydrate on `nimo-2` | `nimo-2` | expected failure |
| T010 | hydrate to `nimo-2` | `nimo-2` | hash match |
| T011 | restored suffix on `nimo-2` | `nimo-2` | timing within target |
| T012 | hot second restored suffix on `nimo-2` | `nimo-2` | no hydration |
| T013 | router restart survival | both | registry/blob survives |
| T014 | `nimo-1` worker reload survival | `nimo-1` | cache can be used again |
| T015 | `nimo-2` worker reload survival | `nimo-2` | cache can be hydrated again |
| T016 | corrupt blob negative | both | no restore |
| T017 | truncated blob negative | both | no restore |
| T018 | wrong model negative | both | no restore |
| T019 | wrong draft config negative | both | no restore |
| T020 | wrong KV type negative | both | no restore |
| T021 | wrong context negative | both | no restore |
| T022 | wrong tenant negative | both | no restore |
| T023 | private-disabled scope | both | no persistence |
| T024 | normal OpenAI chat bypass | selected worker | HTTP 200 |
| T025 | normal OpenAI completion bypass | selected worker | HTTP 200 |
| T026 | cache extension build | selected worker | cache built |
| T027 | cache extension use | selected worker | cache hit |
| T028 | Hermes client smoke | router | client connects |
| T029 | no raw prompt release guard | local | scan passes |
| T030 | non-live regression | local | pass/skips expected |

## Publication Readiness Definition

The repo is ready for the owner to upload to GitHub when:

- license decision is either made or explicitly marked unresolved in the
  release draft;
- local `dist/` archives are isolated or explicitly excluded from any public
  package;
- release index and audit pass with only known local warnings;
- cache-router claims distinguish one-node POC, two-node POC, and production
  distributed cache;
- two-node evidence is summarized with exact host/profile metadata and caveats;
- no raw prompts, raw logs, slot blobs, model files, env values, API tokens,
  private workspaces, or private transcripts are tracked;
- README, guide, claim map, experiment status, and publication blockers point to
  the current evidence paths;
- validation commands and expected skips are documented.

## Hard Stop Conditions

Stop and report before:

- using sudo;
- deleting model files or evidence directories;
- choosing a license;
- publishing, pushing, tagging, uploading, or creating a release;
- downloading large models or datasets;
- using private repos or private prompts;
- running Hermes long stress before cache-router correctness gates pass;
- leaving either host without a healthy intended service;
- claiming production distributed KV cache before correctness and reliability
  tests pass.
