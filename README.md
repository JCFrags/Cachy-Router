# Cachy Router

OpenAI-compatible cache-aware router for trusted-LAN `llama.cpp` worker
clusters.

Cachy Router presents one OpenAI-style endpoint, selects a healthy
`llama-server` worker, and can use worker-local slot files plus a router-owned
durable cache store for explicit prefix-cache experiments. The router can run on
any machine that can reach the worker URLs and sidecars: a worker node, a
controller, a desktop, or a small independent aggregator.

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

Working today:

- OpenAI-compatible `/v1/completions` and `/v1/chat/completions` pass-through.
- Optional cache extension for explicit prefix build/use/refresh flows.
- Worker inventory with one row per `llama-server` worker.
- Router placement on any machine that can reach the workers and sidecars.
- HTTP sidecar transport for worker-local slot upload, hydration, and checks.
- Offline schema/contract validation and replayable example fixtures.
- Public-safe proof-of-concept result summaries.

Still required before broad claims:

- restore-correctness negatives and logit/top-k or deterministic-output checks;
- stronger scheduler policy, queueing, quotas, eviction, and concurrency tests;
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
5. Replace placeholder model paths, worker URLs, sidecar URLs, and identities.
6. Run the setup doctor until the inventory has no failures.
7. Start workers with `--slot-save-path` and matching runtime settings.
8. Start the router on any machine that can reach the worker URLs and sidecars.
9. Point an OpenAI-compatible client at `http://<router-lan-ip>:18080/v1`.

```bash
git clone <repo-url> Cachy-Router
cd Cachy-Router
make check
cp configs/cache-router/workers.example.json configs/cache-router/<deployment>.workers.json
```

Edit the copied inventory:

1. Add or remove worker rows for your cluster.
2. Replace `worker_url` and `sidecar_url` with trusted-LAN addresses.
3. Replace placeholder model paths, runtime settings, and compatibility
   identities.
4. Keep model files, cache blobs, and credentials outside this repository.

Validate an inventory without touching live hosts:

```bash
python3 scripts/cache_router_setup_doctor.py \
  --workers-file configs/cache-router/<deployment>.workers.json \
  --router-host-alias <router-ssh-alias> \
  --router-base-url http://<router-lan-ip>:18080
```

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
  --workers-file configs/cache-router/<deployment>.workers.json
```

Check the endpoint:

```bash
curl -fsS http://<router-lan-ip>:18080/health
curl -fsS http://<router-lan-ip>:18080/v1
curl -fsS http://<router-lan-ip>:18080/v1/models
curl -fsS http://<router-lan-ip>:18080/router/workers
```

Client settings:

```text
Base URL: http://<router-lan-ip>:18080/v1
Model: <model-name-served-by-workers>
Authentication: none in trusted-LAN mode
```

## Configuration

Use `configs/cache-router/workers.example.json` as the canonical editable
template. Local deployment inventories should stay untracked unless they are
sanitized examples.

Each worker row records:

- a stable `worker_id`;
- the worker OpenAI-compatible URL and sidecar URL;
- worker-local model and slot paths;
- shared model/runtime identities used for cache compatibility;
- context size, KV cache types, MTP/speculative settings, and runtime version.

Local filesystem paths may differ across machines. Cache compatibility depends
on shared identities plus matching runtime settings, not on identical path
strings.

## Documentation Map

- `docs/README.md`: reader path and document organization.
- `configs/cache-router/README.md`: worker inventory template notes.
- `docs/architecture/cache-router-setup.md`: live setup and inventory guide.
- `docs/architecture/cache-router-openai-endpoint.md`: endpoint behavior and
  cache extension.
- `docs/architecture/cache-router.md`: architecture overview.
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

Equivalent commands:

```bash
python3 -m py_compile scripts/*.py
python3 scripts/validate_cache_router_contracts.py --json
python3 scripts/cache_router_offline_prototype.py --json
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
