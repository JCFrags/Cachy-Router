# One-Node Cache Router POC

/goal

This document is current when a maintainer can run the smallest same-host
cache-router proof of concept without mistaking it for production distributed
cache routing or two-node restore correctness.

## Scope

This POC is a one-node `nimo-1` experiment. It uses llama.cpp slot save/restore
as the worker primitive and a controller-side script as the minimal router and
registry path.

It may prove:

- a synthetic public prefix can be processed and saved as a slot cache;
- the saved slot can survive a same-host server unload/reload;
- a restored route can reduce prompt processing compared with a cold full
  prompt;
- the result can be recorded with redacted cache-router decision events.

It does not prove:

- cross-node portability;
- production router scheduling;
- tenant-safe shared cache reuse;
- cache correctness beyond the measured same-host continuation;
- MTP speedup versus non-MTP.

## Runner

```bash
python3 scripts/cache_router_one_node_poc.py --help
```

The runner talks to a llama.cpp server with:

- `/health`
- `/slots`
- `/metrics`
- `/tokenize`
- `/completion`
- `POST /slots/{id}?action=save`
- `POST /slots/{id}?action=restore`
- `POST /slots/{id}?action=erase`

The target server must be started with `--slot-save-path`; the normal
`nimo-1:8081` service may not have that flag.

## Data Policy

Track only:

- `README.md`
- `results.json`
- `cache-router-events.jsonl`
- small redacted service snapshots

Do not track:

- slot files;
- raw server logs;
- full prompt text;
- model files;
- private paths beyond necessary public-safe runtime/model identifiers;
- environment values or secrets.

The remote slot directory for this POC is:

```text
/home/connorb/.cache/strix-halo-cache-router-poc/slots/
```

Only cache filenames, byte counts, hashes, timings, and redacted metadata should
enter the source package.

## Interpretation

The primary success target is:

```text
restored_route_prompt_processing_time <= 10% of cold_full_prompt_processing_time
```

The report must calculate:

- prompt-only reduction;
- restore-inclusive reduction;
- whether the reduction reached 90%.

If suffix-only routing is not semantically supported by the current llama.cpp
server API, treat full-prompt replay after slot restore as the meaningful
diagnostic route and label it clearly.

## Current Captured Result

The first captured 30k-token same-host run is recorded at
`data/cache_router_poc/2026-07-01-one-node-30k-slot-restore/`.

Summary:

- result status: `partial_success`;
- cold full `P + Q` prompt-processing time: `92393.891 ms`;
- prefix slot save: `n_saved=30034`, `821501316` bytes, `353.076 ms` API save
  time;
- restored suffix-only route: `n_restored=30034`, `243.066 ms` observed restore
  wall time, `407.138 ms` suffix prompt-processing time;
- prompt-only reduction: `99.559%`;
- restore-inclusive reduction: `99.296%`;
- full-prompt replay after restore: `92634.685 ms` prompt-processing time, so
  replaying the full prompt did not show a shortcut.

This result reaches the timing target for the router-controlled suffix route
after a same-host worker reload. It does not yet prove semantic correctness by
logit/top-k comparison, and it does not prove distributed or cross-node cache
reuse.

## Next POC: Router-PC Blob Store Hop

The production target should behave like a CachyLLama-style cache system with a
remote durable cache tier, not only a local worker slot directory. The current
llama.cpp slot restore API still expects the file to be visible under the
worker's `--slot-save-path`, so the next safe proof should add a sidecar-style
copy/hydration step:

1. worker saves the slot to local NVMe under `--slot-save-path`;
2. sidecar hashes the file and uploads/copies the immutable blob plus manifest
   to a router-owned cache-store directory;
3. registry records the strict cache key, blob hash, byte count, worker/runtime
   metadata, and tenant/conversation scope;
4. later request misses the worker-local hot copy but finds the durable
   router-store blob;
5. sidecar hydrates the blob back into the worker-local `--slot-save-path`;
6. worker restores from the hydrated local file and serves the suffix route.

Directly pointing `--slot-save-path` at a network filesystem may be useful as a
diagnostic, but the safer default is durable remote ownership plus local NVMe
hot copies. That keeps restore latency predictable and makes partial writes,
checksums, eviction, and tenant isolation easier to reason about.

## Current Router-Store Hydration Result

The first captured router-store hydration run is recorded at
`data/cache_router_poc/2026-07-01-router-store-hydration/`.

Summary:

- result status: `success`;
- worker simulation: `worker-a` PID `861019`, `worker-b` PID `861090`;
- `worker-b` slot file existed before hydration: `false`;
- restore-before-hydration probe: expected HTTP `400`;
- router blob SHA256 matched worker-a and hydrated worker-b slot SHA256;
- router ingest copy/hash/manifest time: `776.450 ms`;
- hydration copy/hash time: `789.369 ms`;
- restored suffix route: `n_restored=30034`, `76.489 ms` observed restore
  wall time, `419.245 ms` suffix prompt-processing time;
- prompt-only reduction: `99.546%`;
- hydrate+restore-inclusive reduction: `98.608%`;
- optional worker-b hot-local restore avoided another router copy and kept the
  prompt-only reduction at `99.552%`.

This result proves the same-host simulation of router-owned durable blob
storage plus worker-local hot-cache hydration. It still does not prove real
cross-node transfer or logits/top-k restore correctness.

## Current OpenAI Endpoint Result

The first long-running OpenAI-compatible router endpoint run is recorded at
`data/cache_router_poc/2026-06-30-openai-router-endpoint/`.

Summary:

- result status: `success`;
- router endpoint: `127.0.0.1:18080` on `nimo-1`;
- router-managed worker: `127.0.0.1:18082` with `--slot-save-path`;
- OpenAI base URL through SSH tunnel: `http://127.0.0.1:18080/v1`;
- normal `/v1/chat/completions` and `/v1/completions` pass-through requests
  returned HTTP `200` through the router;
- cache build ran through `/v1/completions` with the `cache_router` extension;
- cache use ran through `/v1/completions` with the `cache_router` extension;
- prefix build: `30033` tokens and `92253.681 ms` prompt-processing time;
- durable blob size: `821501316` bytes;
- worker restart proof: old PID `862117`, new PID `862732`;
- worker-local slot was absent before hydration: `true`;
- hydrated restore: `n_restored=30034`, `75.410 ms` restore wall time,
  `317.026 ms` suffix prompt-processing time;
- prompt-only reduction: `99.656%`;
- hydrate+restore-inclusive reduction: `99.125%`;
- hot-local second use avoided router-store hydration.

See `docs/architecture/cache-router-openai-endpoint.md` for start/stop,
tunnel, client, and endpoint details.
