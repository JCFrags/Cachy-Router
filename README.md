# Cachy Router

OpenAI-compatible cache-aware router for trusted-LAN `llama.cpp` worker
clusters.

Cachy Router presents one OpenAI-style endpoint, selects a ready
`llama-server` worker whose `/health` and `/v1/models` probes pass, and can use
worker-local slot files plus a router-owned durable cache store for explicit
prefix-cache experiments. The router can run on any machine that can reach the
worker URLs and sidecars: a worker node, a controller, a desktop, or a small
independent aggregator.

```text
OpenAI-compatible client
  -> Cachy Router endpoint on any reachable host
  -> one of N llama.cpp workers
  -> worker-local hot slot cache
  -> router-owned durable cache blob store
```

## Status

This is an early public split of a working trusted-LAN MVP. The code and
examples support an inventory with any number of workers, but this is not
production infrastructure yet.

Acceptance snapshot (`python3 scripts/validate_acceptance_metrics.py --json`):
`168` total metrics: `154 done`, `10 live-gated`, `2 partial`, `0 planned`,
`2 blocked-by-upstream`.

Working today:

- OpenAI-compatible route surface and offline proxy smoke coverage (`done`
  evidence); deployment-wide `/v1/models`, `/v1/completions`, and
  `/v1/chat/completions` success remains `live-gated`.
- Normal non-cached streaming pass-through for completion and chat requests is
  covered by offline smoke (`done`); `cache_router.mode=bypass` may stream after
  the extension is stripped, while cached build/use modes are rejected clearly
  when `stream=true`.
- Router-generated responses carry router-owned request/trace IDs and
  `X-Cache-Router-Worker: none`; routed worker responses carry the selected
  worker ID (`done` offline endpoint-family smoke).
- Explicit cache extension mechanics for prefix build/use/refresh/auto are
  covered by offline fake-worker smoke (`done` for router control flow and
  artifact publication). One no-MTP trusted-LAN suffix benchmark gate is now
  `done` for prompt-token reduction and true TTFT, and one no-MTP trusted-LAN
  restore correctness gate has a scoped live pass; MTP-enabled restore
  correctness, logits/top-k validation, and distributed correctness remain
  `live-gated`, `partial`, or `blocked-by-upstream` as listed in the acceptance
  matrix.
- Worker inventory with one row per `llama-server` worker (`done` for 1, 2, 3,
  and 8+ worker offline inventory checks).
- Worker readiness gating skips model-loading workers until `/v1/models`
  reports the configured model (`done` offline evidence).
- Normal proxy fallback handles an offline worker-disconnect probe by serving
  the fallback worker, recording a bounded `worker_unavailable` decision event,
  and routing the next request to the remaining ready worker (`done` offline
  evidence, not a live model-process crash or production failover test).
- Tenant/scope cache policy is enforced before restore or generation; denied
  cross-tenant and cross-conversation cache use is admin-audited and
  client-masked as a scoped cache miss, while `scope=tenant` is the explicit
  same-tenant broader reuse mode (`done` offline evidence, not internet-safety
  or production isolation).
- Cache use and auto mode require strict lookup material: either
  `cache_router.cache_key_hash` or a non-empty `cache_router.prefix_text` from
  which the daemon derives the request/runtime cache key. Cache-id-only restore
  is rejected before lookup; selected manifests also self-check canonical
  strict-key material before restore (`done` offline evidence).
- Optional per-worker router queueing can be enabled for normal proxy traffic;
  offline smoke coverage verifies queue depth affects scheduling, queue wait
  metrics are exposed, and queue-full requests receive bounded `503` JSON
  errors before backend forwarding while rejected-load RSS remains within the
  current 110% gate. The offline performance probe also verifies a target
  `N workers * configured active slots` burst against two loopback workers
  (`done` offline evidence). `make check-scheduler-stress` runs the separate
  600-second loopback scheduler stress probe for
  `2 * N workers * configured active slots` concurrent waves,
  schema-valid decision events, and no traffic to an unavailable worker (`done`
  offline evidence, not production load-balancing quality or long-soak memory
  proof).
- Offline loopback performance probes check normal proxy overhead against a
  p95 <50 ms gate and local registry lookup against a p95 <25 ms gate (`done`
  evidence for synthetic local probes only, not live network or generation
  performance).
- Operator-run restore-correctness and long-soak harnesses exist for the
  remaining live gates: `scripts/cache_router_correctness_probe.py` compares
  deterministic cold/restored output digests, token hashes when `/tokenize`
  exposes token lists, and router decision events, then emits redacted
  validation JSONL; `scripts/cache_router_suffix_benchmark_gate.py` enforces
  repeatable prompt-token reduction, non-streaming first-token proxy math, and
  opt-in true TTFT measurement via `cache_router.measure_true_ttft=true`, with a
  no-MTP trusted-LAN 10-run pass now recorded for the suffix benchmark and a
  no-MTP trusted-LAN restore-correctness gate now recorded for deterministic,
  boundary, conversation, tool-output, near-context, and branch/retry cases;
  while `scripts/cache_router_long_soak_probe.py` runs the 8-hour RSS-growth
  gate offline against loopback workers. Their short self-tests are covered by
  `make check`; MTP-enabled correctness, restart/two-node live restore,
  logits/top-k validation, and 8-24 hour soak evidence remain required before
  broad production or distributed-cache claims.
- Router placement on any machine that can reach the workers and sidecars
  (`done` documentation claim).
- HTTP sidecar transport for worker-local slot upload, hydration, and checks
  (`done` offline sidecar and transport smoke evidence).
- Offline durable-store tenant deletion, content-addressed blob GC safety,
  persisted registry leases, and redacted append-only registry audit JSONL
  checks (`done` for local operator tooling; production retention policy,
  tamper-proof audit storage, and distributed ordering remain outside the
  current claim).
- Operator-managed encrypted cache root metadata for router-owned durable
  blobs is generated, documented, schema-checked, setup-doctor validated, and
  audited with `scripts/cache_router_store_audit.py --require-encryption-at-rest`
  (`done` for offline declaration and audit gates; this is not Python-level
  blob encryption, KMS integration, or platform cryptography proof).
- Offline schema/contract validation and replayable example fixtures (`done`).
- Fresh-checkout setup docs are mechanically checked against the offline doctor
  path, public inventory template, trusted-LAN warnings, and live-command
  placeholder rules (`done` docs evidence, not live deployment proof).
- Architecture docs are mechanically checked against implemented route, cache,
  security, compatibility, and planned-only caveats (`done` docs evidence).
- Public-safe proof-of-concept result summaries (`done` as redacted evidence,
  including the scoped no-MTP suffix TTFT and restore-correctness passes).

Still required before broad claims:

- MTP-enabled restore-correctness runs and logit/top-k runtime validation support;
- stronger scheduler policy, quotas, production retention/GC orchestration,
  live production load tests, and long soak tests;
- untrusted-network hardening, tenant isolation, and operational packaging.

The default documented mode is unauthenticated on a trusted LAN. Do not expose a
router, worker, or sidecar directly to the public internet or any untrusted
network.

## Install

The router scripts use the Python standard library. No package install is
required for offline checks or the helper scripts.

Prerequisites for a live deployment:

- Python 3 on the controller, router host, and worker hosts.
- A compatible `llama-server` binary on every worker.
- Matching model files on every worker, stored outside this repository.
- Network reachability from the router to each worker URL and sidecar URL.
- SSH reachability only if you use the stack helper to start remote processes.

Models, slot files, cache blobs, raw logs, runtime state, and credentials should
stay outside Git.

## Quick Start

1. Clone the repository and enter the checkout.
2. Run the offline checks with `make check`.
3. Copy `configs/cache-router/workers.example.json` to a local deployment file.
4. Add one worker row per machine. There is no two-worker limit.
5. Replace placeholder worker URLs, sidecar URLs, model/runtime launch paths,
   and encrypted-volume metadata. Leave `strict_metadata_auto` and
   `strict_metadata_force_runtime` enabled for normal deployments so the daemon
   computes strict cache-key fields from live worker metadata.
6. Place router-owned durable blobs under an operator-managed encrypted cache
   root, then replace `durable_blob_encryption_at_rest.volume_id_hash` with a
   non-secret SHA-256 digest of the encrypted volume identifier.
7. Run the setup doctor until the inventory has no failures.
8. Start workers with `--slot-save-path` and matching runtime settings.
9. Start the router on any machine that can reach the worker URLs and sidecars.
10. Point an OpenAI-compatible client at `http://<router-lan-ip>:18080/v1`.

```bash
git clone <repo-url> Cachy-Router
cd Cachy-Router
make check
cp configs/cache-router/workers.example.json configs/cache-router/<deployment>.workers.json
```

Edit the copied inventory:

1. Add or remove worker rows for your cluster.
2. Replace `worker_url` and `sidecar_url` with trusted-LAN addresses.
3. Replace placeholder model paths, worker URLs, sidecar URLs, and runtime
   launch settings. The default template computes strict cache-key metadata at
   router startup instead of asking you to hand-enter strict hashes.
4. Replace `durable_blob_encryption_at_rest.volume_id_hash` with a digest of
   the encrypted cache-root volume identifier; do not put raw volume names,
   keys, or credentials in the inventory.
5. Keep model files, cache blobs, and credentials outside this repository.

Validate an inventory without touching live hosts:

```bash
python3 scripts/cache_router_setup_doctor.py \
  --workers-file configs/cache-router/<deployment>.workers.json \
  --router-host-alias <router-ssh-alias> \
  --router-base-url http://<router-lan-ip>:18080
```

The following commands are live operations. Run them only with an
operator-supplied deployment inventory, reachable private hosts, and a trusted
private LAN boundary.

Start workers from an inventory:

```bash
python3 scripts/cache_router_remote_stack.py start-workers \
  --workers-file configs/cache-router/<deployment>.workers.json \
  --worker-bind-host 0.0.0.0 \
  --worker-transport http \
  --sidecar-bind-host 0.0.0.0 \
  --allow-unauthenticated-lan \
  --llama-server <path-to-llama-server> \
  --model <path-to-main-gguf> \
  --mtp-model <path-to-draft-gguf> \
  --ctx-size 65536
```

Start or restart the router:

```bash
python3 scripts/cache_router_remote_stack.py restart-router \
  --remote-host <router-ssh-alias> \
  --router-host 0.0.0.0 \
  --no-router-auth \
  --allow-unauthenticated-lan \
  --durable-blob-encryption-mode operator_managed_encrypted_filesystem \
  --durable-blob-encryption-evidence-basis operator_attestation \
  --durable-blob-encryption-volume-id-hash <encrypted-volume-id-sha256> \
  --durable-blob-encryption-key-owner operator \
  --workers-file configs/cache-router/<deployment>.workers.json
```

For authenticated private-LAN production-mode posture, use router auth instead
of the no-key trusted-LAN override. This mode is still not a claim of broad
production readiness. The helper creates or reuses a remote token file with
owner-only permissions and starts the daemon with `--production-mode`; admin
endpoints are disabled unless explicitly allowed:

```bash
python3 scripts/cache_router_remote_stack.py restart-router \
  --remote-host <router-ssh-alias> \
  --router-host 0.0.0.0 \
  --router-auth \
  --production-router-mode \
  --durable-blob-encryption-mode operator_managed_encrypted_filesystem \
  --durable-blob-encryption-evidence-basis operator_attestation \
  --durable-blob-encryption-volume-id-hash <encrypted-volume-id-sha256> \
  --durable-blob-encryption-key-owner operator \
  --workers-file configs/cache-router/<deployment>.workers.json
```

Check a trusted-LAN no-auth endpoint:

```bash
curl -fsS http://<router-lan-ip>:18080/health
curl -fsS http://<router-lan-ip>:18080/v1
curl -fsS http://<router-lan-ip>:18080/v1/models
curl -fsS http://<router-lan-ip>:18080/router/workers
curl -fsS http://<router-lan-ip>:18080/metrics
```

Check an authenticated private-LAN production-mode endpoint:

```bash
TOKEN='<router-token>'
curl -fsS http://<router-lan-ip>:18080/health
curl -fsS -H "Authorization: Bearer ${TOKEN}" http://<router-lan-ip>:18080/v1
curl -fsS -H "Authorization: Bearer ${TOKEN}" http://<router-lan-ip>:18080/v1/models
curl -fsS -H "Authorization: Bearer ${TOKEN}" http://<router-lan-ip>:18080/router/workers
curl -fsS -H "Authorization: Bearer ${TOKEN}" http://<router-lan-ip>:18080/metrics
```

Client settings:

```text
Base URL: http://<router-lan-ip>:18080/v1
Model: <model-name-served-by-workers>
Authentication: none in trusted-LAN mode
Authenticated mode: pass the configured bearer token or `X-API-Key`
```

Run the Core API worker/model matrix against an operator-supplied router:

```bash
python3 scripts/cache_router_live_endpoint_matrix.py --json \
  --router-url http://<router-lan-ip>:18080 \
  --workers-file configs/cache-router/<deployment>.workers.json \
  --out-dir runtime/cache-router-core-api-matrix/<public-safe-run-id>
```

Run restore-correctness gates against an operator-supplied trusted-LAN router:

```bash
python3 scripts/cache_router_correctness_probe.py --json \
  --router-url http://<router-lan-ip>:18080 \
  --model <model-name-served-by-workers> \
  --runs 10 \
  --cache-id-prefix <public-safe-run-id> \
  --out-dir runtime/cache-router-correctness/<public-safe-run-id>
```

The correctness probe writes redacted `summary.json`,
`validation-results.jsonl`, and `decision-events.jsonl` files under the chosen
ignored runtime directory. Validate retained JSONL before summarizing it:

```bash
python3 scripts/validate_cache_router_contracts.py --json \
  --events runtime/cache-router-correctness/<public-safe-run-id>/decision-events.jsonl \
  --validations runtime/cache-router-correctness/<public-safe-run-id>/validation-results.jsonl
```

Run the suffix-route benchmark gate against an operator-supplied router:

```bash
python3 scripts/cache_router_suffix_benchmark_gate.py --json \
  --router-url http://<router-lan-ip>:18080 \
  --model <model-name-served-by-workers> \
  --runs 10 \
  --max-tokens 1 \
  --out-dir runtime/cache-router-suffix-benchmark/<public-safe-run-id>
```

This gate enforces fixed-probe prompt-eval token reduction and timing. It keeps
the non-streaming one-token first-token proxy as a diagnostic, measures cold TTFT
with normal streaming pass-through, and asks cached requests for
`cache_router.measure_true_ttft=true`. Proxy-only success is reported as
`proxy_benchmark_ok=true` with top-level `ok=false`; final acceptance requires
true first-token timing for cached suffix routes.

Run the 8-hour offline loopback soak gate and keep raw output outside the repo:

```bash
python3 scripts/cache_router_long_soak_probe.py --json \
  --duration-seconds 28800 \
  --baseline-after-seconds 3600 \
  --sample-interval-seconds 60 \
  > /tmp/cache-router-long-soak-<public-safe-run-id>.json
```

## Configuration

Use `configs/cache-router/workers.example.json` as the canonical editable
template. Local deployment inventories should stay untracked unless they are
sanitized examples.

Each worker row records:

- a stable `worker_id`;
- the worker OpenAI-compatible URL and sidecar URL;
- worker-local model and slot paths;
- runtime launch hints such as context size, KV cache type, and
  MTP/speculative settings;
- `strict_metadata_auto` and `strict_metadata_force_runtime`, which tell the
  daemon to replace stale or missing strict cache-key fields with computed
  runtime metadata before cache build/use.

Local filesystem paths may differ across machines. Cache compatibility depends
on the daemon's computed runtime identity and strict metadata, not on identical
path strings or hand-entered labels.

## Documentation Map

- `docs/README.md`: reader path and document organization.
- `configs/cache-router/README.md`: worker inventory template notes.
- `docs/architecture/cache-router-setup.md`: live setup and inventory guide.
- `docs/architecture/cache-router-openai-endpoint.md`: endpoint behavior and
  cache extension.
- `docs/architecture/cache-router.md`: architecture overview.
- `docs/architecture/final-acceptance-metrics.md`: testable 90-100% final
  build acceptance matrix and current evidence status.
- `docs/benchmark-claim-map.md`: what the retained evidence does and does not
  support.
- `evidence/cache-router-results-summary.md`: public-safe result summary.
- `evidence/README.md`: evidence retention rules.

Historical lab notes should stay outside this public repo unless they have been
redacted into a small public summary.

## Offline Checks

```bash
make check
```

`make check` proves the current working tree. For release proof after all
intended public files are tracked and committed, run the clean-checkout gate:

```bash
make check-clean-checkout
```

That target is local-only. It refuses dirty worktrees, creates a temporary
detached worktree at `HEAD`, runs `make check` there, and removes the temporary
worktree afterward. It does not contact remote hosts.

Equivalent commands:

```bash
python3 -B -c 'import ast, pathlib; [ast.parse(p.read_text(encoding="utf-8"), filename=str(p)) for p in pathlib.Path("scripts").glob("*.py")]'
python3 scripts/validate_cache_router_contracts.py --json
python3 scripts/replay_cache_router_decisions.py --json
python3 scripts/cache_router_offline_prototype.py --json
python3 scripts/cache_router_setup_doctor.py --workers-file configs/cache-router/workers.example.json --json
python3 scripts/cache_router_setup_doctor_matrix_test.py
python3 scripts/cache_router_daemon_smoke_test.py
python3 scripts/cache_router_sidecar_smoke_test.py
python3 scripts/cache_router_transport.py --self-test
python3 scripts/cache_router_store_audit.py --self-test --json
python3 scripts/cache_router_performance_probe.py --json
python3 scripts/cache_router_correctness_probe.py --self-test --json
python3 scripts/cache_router_long_soak_probe.py --self-test --json
python3 scripts/cache_router_live_endpoint_matrix.py --self-test --json
python3 scripts/cache_router_suffix_benchmark_gate.py --self-test --json
python3 scripts/cache_router_public_hygiene_scan.py --self-test --json
python3 scripts/validate_cache_router_setup_docs.py
python3 scripts/validate_cache_router_endpoint_docs.py
python3 scripts/validate_cache_router_architecture_doc.py
python3 scripts/validate_acceptance_metrics.py --json
python3 scripts/validate_cache_router_claim_map.py --json
python3 scripts/cache_router_release_gap_report.py --summary
```

Run the long offline scheduler stress proof separately when validating a release
candidate:

```bash
make check-scheduler-stress
```

These checks do not contact remote hosts.

## Repository Hygiene

This repository intentionally excludes:

- GGUF/model/checkpoint files;
- remote slot files and durable cache blobs;
- raw prompts, raw requests, raw responses, and raw logs;
- runtime PID files, auth-token files, shell history, and environment captures;
- credentials, provider configs, private keys, and `.env` files;
- package archives and generated build output.

See `docs/raw-evidence-retention-audit.md` for evidence handling rules.

## License

MIT. See `LICENSE`.
