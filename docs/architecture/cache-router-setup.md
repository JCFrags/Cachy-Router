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
first requires worker `/health` and worker `/v1/models` readiness, uses `/slots`
state to prefer ready idle workers, then cache-local workers, and can hydrate a
durable blob into another idle worker when the hot worker is busy and fallback is
allowed. Worker `/v1/models` HTTP 503 loading responses are treated as warming,
not as permanent failures. Optional router-side per-worker queueing can be
enabled for normal proxy traffic with `--queue-limit-per-worker` and
`--queue-wait-timeout`; when enabled, queue depth participates in scheduling and
queue-full requests are rejected with bounded OpenAI-shaped `503` JSON before
backend forwarding. This is enough for the current MVP, but production
load-balancing policy, quotas, RSS-bound overload tests, and weighted scheduling
remain future work.

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
  --cache-root /home/<user>/.cache/strix-halo-cache-router \
  --durable-blob-encryption-mode operator_managed_encrypted_filesystem \
  --durable-blob-encryption-evidence-basis operator_attestation \
  --durable-blob-encryption-volume-id-hash <encrypted-volume-id-sha256> \
  --durable-blob-encryption-key-owner operator \
  --strict-metadata-force-runtime \
  --llama-server /home/<user>/llama.cpp/build/bin/llama-server \
  --model /models/<main-model>.gguf \
  --mtp-model /models/<draft-model>.gguf \
  --mtp-enabled \
  --kv-unified-mode \
  --ctx-size 65536 \
  --cache-type-k q8_0 \
  --cache-type-v q8_0
```

Add more `--worker` arguments for more worker nodes. Use
`worker-id@ssh-alias=lan-ip` when the SSH alias differs from the public worker
ID. The generated worker slot paths must be absolute worker-local paths before
the setup doctor can pass; use an absolute `--cache-root`. Generated
inventories enable runtime strict metadata derivation by default, so the daemon
computes cache-key compatibility fields from `/v1/models`, `/props`, and
`/tokenize` at startup instead of relying on hand-entered strict hashes. If you
intentionally pin exact operator-supplied hashes or ABI IDs, disable
`strict_metadata_force_runtime` for that deployment and keep the pinned values
complete.

## Durable Cache Storage

The top-level `cache_storage` inventory block describes the router-owned
durable blob store. For cache build/use, put this store on an
operator-managed encrypted cache root, such as a local encrypted filesystem or
platform encrypted volume mounted outside the Git checkout:

```json
{
  "cache_storage": {
    "cache_root": "/home/<user>/.cache/strix-halo-cache-router",
    "durable_blob_encryption_at_rest": {
      "required": true,
      "mode": "operator_managed_encrypted_filesystem",
      "evidence_basis": "operator_attestation",
      "volume_id_hash": "<encrypted-volume-id-sha256>",
      "key_owner": "operator"
    }
  }
}
```

`volume_id_hash` is a SHA-256 digest of an operator-chosen encrypted volume
identifier. Keep raw volume names, encryption keys, recovery keys,
credentials, and token files out of the inventory. The setup doctor validates
the metadata shape offline and warns while the public placeholder is still
present.

Router-created manifests can carry the same operator attestation when the
router is started with the `--durable-blob-encryption-*` flags. The offline
store audit can then reject active durable manifests that do not carry valid
metadata:

```bash
python3 scripts/cache_router_store_audit.py \
  --cache-root /home/<user>/.cache/strix-halo-cache-router \
  --require-encryption-at-rest \
  --json
```

This is an audit gate for an operator-managed encrypted cache root. It is not
Python-level blob encryption, KMS integration, or proof that the underlying
platform encryption is correctly configured.

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
  "slot_save_path": "/home/<user>/.cache/strix-halo-cache-router/workers/worker-a/slots",
  "slot_id": 0,
  "strict_metadata_auto": true,
  "strict_metadata_force_runtime": true,
  "model": "<model-name>",
  "model_path": "/models/<main-model>.gguf",
  "llama_server_path": "/home/<user>/llama.cpp/build/bin/llama-server",
  "ctx_size": 65536,
  "kv_unified_mode": true,
  "cache_type_k": "q8_0",
  "cache_type_v": "q8_0",
  "mtp_enabled": true,
  "spec_draft_model_path": "/models/<draft-model>.gguf",
  "spec_draft_n_max": 3,
  "spec_draft_n_min": 0,
  "spec_draft_p_split": "0.10",
  "spec_draft_p_min": "0.60",
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
may differ across machines. With `strict_metadata_auto` and
`strict_metadata_force_runtime` enabled, these local paths and launch hints are
not treated as hand-entered cache proof. At router startup the daemon computes
the strict cache-key fields from worker runtime APIs and tokenizer probes. Those
computed fields include model and draft hashes, GGUF tensor manifest surrogate,
tokenizer hash, chat template hash, tools schema hash, system prompt hash,
model architecture, special-token policy, llama.cpp source/cache ABI, local
patchset, backend/driver lane, context and KV settings, RoPE/YaRN metadata,
MTP/speculative config, parallel/sequence lane, and template/reasoning modes.
Local paths are kept as provenance/debug metadata, not as the shared
compatibility boundary.

Validate an inventory without touching live hosts:

```bash
python3 scripts/cache_router_setup_doctor.py \
  --workers-file configs/cache-router/<deployment>.workers.json \
  --router-host-alias <router-ssh-alias> \
  --router-base-url http://<router-lan-ip>:18080
```

The example file intentionally contains placeholders, so the doctor reports
warnings until you replace deployment URLs, paths, and encryption metadata.
Because the template enables runtime-forced strict metadata derivation, missing
manual strict hashes are reported as informational derivation posture rather
than as fields you must fill by hand. A deployment inventory can be tracked for
a public/reproducible deployment or kept local for private hostnames, but a real
inventory should have zero failures before runtime work.
After workers and the router are running, add `--live` to check `/health` on
the router, workers, and HTTP sidecars without restarting or mutating any
process.

The default examples below bind the router, workers, and sidecars to
`0.0.0.0` without authentication for a trusted LAN. Do not use these flags on a public
interface or untrusted network.

Commands in the runtime sections below are live operations. Run them only with
an operator-supplied deployment inventory, reachable worker/router hosts, and a
trusted LAN security boundary.

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
was used to start workers. When the inventory has `cache_storage.cache_root`,
the remote helper stages the router and durable store under that root unless
`--remote-cache-root` is passed explicitly:

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

Add `--disable-router-admin-endpoints` if the OpenAI-compatible client surface
should remain available but `/router/*` inspection routes and `/metrics` should
be disabled.

For authenticated private-LAN production-mode posture, use router auth instead
of the no-key trusted-LAN override. This is an authenticated deployment mode,
not a broad production-readiness claim:

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

In production mode, `/health` remains available with reduced detail, `/v1/*`
requires `Authorization: Bearer <router-token>` or `X-API-Key: <router-token>`,
and admin endpoints are disabled unless explicitly allowed.

Check it:

```bash
curl -fsS http://<router-lan-ip>:18080/health
curl -fsS http://<router-lan-ip>:18080/v1/models
curl -fsS http://<router-lan-ip>:18080/router/workers
curl -fsS http://<router-lan-ip>:18080/metrics
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
specific worker and want the request to fail instead of using another ready
configured worker.

## Retained Evidence

Public-safe proof-of-concept summaries live in
`evidence/cache-router-results-summary.md`. Raw run folders should stay private
unless they have been redacted and reviewed.

Current public claims should remain narrow: trusted-LAN no-key use, normal
OpenAI-compatible pass-through, explicit suffix-route cache flows, HTTP sidecar
hydration, and bounded availability-aware routing probes. Production hardening
still needs production retention/GC orchestration, concurrency/load policy, tenant hardening,
untrusted-network policy, and restore-correctness negative tests.
