# Cache Router Setup

/goal

This document is complete when a GitHub reader can understand the moving parts,
create an N-worker inventory, start a trusted-LAN router stack, and point an
OpenAI-compatible client at the router without knowing the history of the POC
scripts.

## Topology

The router is an aggregator. It can run on a worker node or on an independent
PC. Workers are the machines that actually run `llama-server`.

```text
OpenAI-compatible client
  -> router host:18080
  -> one of N worker llama-server processes:18082
  -> worker-local slot sidecar:18083
  -> worker-local NVMe hot slots
  -> router-owned durable blob store
```

The router can run on any machine that can reach the worker URLs and sidecar
URLs. It does not have to run on a worker node.

The worker config is a list. Use one worker for a local smoke test, two workers
for a small cluster, or more workers by adding more rows. There is no intended
two-worker limit.

Current scheduling is availability-aware at the worker-slot level: the router
uses `/slots` state to prefer healthy idle workers, then cache-local workers,
and can hydrate a durable blob into another idle worker when the hot worker is
busy and fallback is allowed. This is enough for the current MVP, but production
queueing, quotas, tenant policy, and weighted scheduling remain future work.

## Worker Requirements

Each worker should have:

- the same compatible `llama-server` build and cache ABI;
- the same model bytes and MTP/draft model bytes;
- matching context, KV type, MTP/speculative, template, and slot/cache flags;
- a `llama-server` worker port, default `18082`;
- `--slot-save-path` pointing to worker-local storage;
- a slot sidecar port, default `18083`;
- model files outside the Git checkout.

The router should have:

- network access to every worker's `worker_url`;
- network access to every worker's sidecar URL;
- a durable cache root outside the Git checkout;
- an OpenAI-compatible bind address, default `18080`.

## Worker Inventory

Start from:

```text
configs/cache-router/workers.example.json
```

For a new deployment, copy the example file and replace the worker rows with
your own hostnames, trusted-LAN URLs, and worker-local slot paths. Other tracked
inventory files are retained evidence or private deployment examples; do not use
them as the default install path.

You can also generate a starter inventory instead of hand-editing JSON:

```bash
python3 scripts/cache_router_make_inventory.py \
  --worker worker-a=<worker-a-lan-ip> \
  --worker worker-b=<worker-b-lan-ip> \
  --output configs/cache-router/<deployment>.workers.json \
  --llama-server /home/<user>/llama.cpp/build/bin/llama-server \
  --model /models/<main-model>.gguf \
  --model-identity <shared-main-model-identity> \
  --mtp-model /models/<draft-model>.gguf \
  --mtp-model-identity <shared-draft-model-identity> \
  --ctx-size 65536
```

Add more `--worker` arguments for more worker nodes. Use
`worker-id@ssh-alias=lan-ip` when the SSH alias differs from the public worker
ID.

Recommended setup flow for a fresh GitHub checkout:

1. Generate or copy `configs/cache-router/workers.example.json` to a local
   deployment file such as `configs/cache-router/<deployment>.workers.json`.
2. Include one row for every worker host that should run a model.
3. Run the setup doctor against that file until there are no failures.
4. Start router-managed workers from the inventory.
5. Start or restart the router with the same worker inventory file.
6. Point the OpenAI-compatible client at the router, not at individual workers.

One worker row looks like:

```json
{
  "worker_id": "worker-a",
  "ssh_host": "<worker-a-ssh-alias>",
  "worker_url": "http://<worker-a-lan-ip>:18082",
  "slot_save_path": "/home/<user>/.cache/cachy-router/workers/worker-a/slots",
  "slot_id": 0,
  "model": "<model-name>",
  "model_identity": "<same-compatible-model-identity-on-every-worker>",
  "model_path": "/models/<main-model>.gguf",
  "model_file_size": 0,
  "llama_server_path": "/home/<user>/llama.cpp/build/bin/llama-server",
  "llama_server_version": "<llama.cpp-version-or-commit>",
  "ctx_size": 65536,
  "cache_type_k": "q8_0",
  "cache_type_v": "q8_0",
  "mtp_enabled": true,
  "spec_draft_model_identity": "<same-compatible-draft-model-identity-on-every-worker>",
  "spec_draft_model_path": "/models/<draft-model>.gguf",
  "spec_draft_model_size": 0,
  "transport": {
    "kind": "http",
    "sidecar_url": "http://<worker-a-lan-ip>:18083"
  }
}
```

Use one row per worker. Add a third, fourth, or tenth worker by adding another
row. The `http` transport is the preferred MVP path because it lets the router
run independently from the worker nodes. The older `local` transport is only
for colocated router/worker tests. The `ssh` transport is useful in controlled
deployments but requires the router host itself to have SSH access to workers.

`model_path` and `spec_draft_model_path` are worker-local filesystem paths and
may differ across machines. `model_identity` and `spec_draft_model_identity`
are the compatibility identities the router uses for cache keys. Workers that
should share cache blobs must use the same identities and matching model bytes,
runtime version, context size, KV cache types, MTP/speculative settings, and
template behavior. Local paths are kept as provenance/debug metadata, not as
the shared compatibility boundary.

Before starting anything, validate the inventory:

```bash
python3 scripts/cache_router_setup_doctor.py \
  --workers-file configs/cache-router/<deployment>.workers.json \
  --router-host-alias <router-ssh-alias> \
  --router-base-url http://<router-lan-ip>:18080
```

The example file intentionally contains placeholders, so the doctor reports
warnings until you replace them. A deployment inventory can be tracked for a
public/reproducible deployment or kept local for private hostnames, but a real
inventory should have zero failures before runtime work. After workers and the
router are running, add `--live` to check `/health` on the router, workers, and
HTTP sidecars without restarting or mutating any process.

The default examples below bind the router, workers, and sidecars to
`0.0.0.0` without authentication for a trusted LAN. Do not use these flags on a public
interface or untrusted network.

## Start Workers

The easiest path is to start every worker in the inventory:

```bash
python3 scripts/cache_router_remote_stack.py start-workers \
  --workers-file configs/cache-router/<deployment>.workers.json \
  --worker-bind-host 0.0.0.0 \
  --worker-transport http \
  --sidecar-bind-host 0.0.0.0 \
  --allow-unauthenticated-lan \
  --llama-server <path-to-llama-server> \
  --model <path-to-main-gguf-shard> \
  --mtp-model <path-to-mtp-gguf> \
  --ctx-size 65536
```

Check every worker:

```bash
python3 scripts/cache_router_remote_stack.py workers-status \
  --workers-file configs/cache-router/<deployment>.workers.json
```

Stop every owned worker from the inventory:

```bash
python3 scripts/cache_router_remote_stack.py stop-workers \
  --workers-file configs/cache-router/<deployment>.workers.json
```

## Start One Worker

Run once per worker host, adjusting paths for that host:

```bash
python3 scripts/cache_router_remote_stack.py start-worker \
  --remote-host <worker-ssh-alias> \
  --worker-id <worker-id> \
  --worker-bind-host 0.0.0.0 \
  --worker-transport http \
  --worker-sidecar-url http://<worker-lan-ip>:18083 \
  --sidecar-bind-host 0.0.0.0 \
  --sidecar-port 18083 \
  --allow-unauthenticated-lan \
  --llama-server <path-to-llama-server> \
  --model <path-to-main-gguf-shard> \
  --mtp-model <path-to-mtp-gguf> \
  --ctx-size 65536
```

Check a worker:

```bash
python3 scripts/cache_router_remote_stack.py worker-status \
  --remote-host <worker-ssh-alias> \
  --worker-id <worker-id> \
  --worker-transport http \
  --sidecar-bind-host 0.0.0.0
```

## Start The Router

Run this on the controller from the repo checkout. The router host can be a
worker node or a separate PC reachable by SSH. Use the same inventory file that
was used to start workers:

```bash
python3 scripts/cache_router_remote_stack.py restart-router \
  --remote-host <router-ssh-alias> \
  --router-host 0.0.0.0 \
  --no-router-auth \
  --allow-unauthenticated-lan \
  --workers-file configs/cache-router/<deployment>.workers.json
```

Check it:

```bash
curl -fsS http://<router-lan-ip>:18080/health
curl -fsS http://<router-lan-ip>:18080/v1/models
curl -fsS http://<router-lan-ip>:18080/router/workers
```

OpenAI-compatible client settings:

```text
Base URL: http://<router-lan-ip>:18080/v1
Model: <model-name-served-by-workers>
Authentication: none for trusted-LAN mode
```

If a client UI insists on an API-key field, use any harmless placeholder. The
current no-key router path does not validate that field.

The router defaults cached `use` requests to `allow_fallback=true`. Set
`cache_router.allow_fallback=false` only when you are intentionally testing one
specific worker and want the request to fail instead of using another healthy
configured worker.

## Retained Evidence

Public-safe proof-of-concept summaries live in
`evidence/cache-router-results-summary.md`. Raw run folders should stay private
unless they have been redacted and reviewed.

Current public claims should remain narrow: trusted-LAN no-key use, normal
OpenAI-compatible pass-through, explicit suffix-route cache flows, HTTP sidecar
hydration, and bounded availability-aware routing probes. Production hardening
still needs concurrency/load policy, cache eviction, untrusted-network policy,
and restore-correctness negative tests.
