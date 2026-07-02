# Cache Router Results Summary

This file is a public-safe summary of internal proof-of-concept runs. It omits
hostnames, LAN addresses, user paths, PIDs, raw prompts, slot files, cache blobs,
and logs.

## What Was Demonstrated

| result | status | summary |
|---|---|---|
| One-node slot restore | Exploratory success | A long synthetic prefix was processed once, saved as a `llama.cpp` slot file, restored after a worker reload, and then used for a suffix-only route. Prompt processing dropped by more than 90% for the suffix route. |
| Router-owned durable store | Exploratory success | A worker-local slot file was ingested into a router-owned blob store, then hydrated into a fresh worker-local slot directory before restore. |
| OpenAI-compatible endpoint | Exploratory success | The router accepted normal OpenAI-style completion and chat requests and supported an explicit cache extension for prefix build/use flows. |
| Two-worker LAN hydration | Exploratory success | A cache built on one compatible worker was hydrated to another compatible worker through the router/sidecar path. |
| Busy-worker fallback | Exploratory success | A bounded probe showed the router selecting an available worker when another configured worker was busy or unavailable. |

## What Is Not Proven Yet

- This is not production distributed-cache correctness.
- Logit/top-k restore validation and negative correctness tests remain required.
- The current implementation is designed for trusted private LANs, not the
  public internet.
- The router has not been hardened for quotas, adversarial tenants, eviction,
  concurrent load, or untrusted networks.
- Model files, runtime flags, tokenizer/template state, KV types, and cache
  compatibility settings must match for safe restore.

## Publication Rule

Use the table above for public claims. Keep raw run folders in private evidence
storage unless they have been explicitly redacted and reviewed.
