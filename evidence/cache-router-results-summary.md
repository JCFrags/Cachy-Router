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
| No-MTP suffix TTFT gate | Scoped live pass | A trusted-LAN no-MTP worker passed the 10-run suffix benchmark gate with 0 fallbacks, 0 restore errors, 0 full-reprocess suspicions, `local_nvme` cache hits, about 99.85% prompt-eval token reduction, and about 96.9% median true-TTFT improvement. |
| No-MTP restore correctness gate | Scoped live pass | A trusted-LAN no-MTP worker passed the restore correctness gate with `--runs 10`, `--no-fallback`, and router-side deterministic recompute validation: 60 restored comparisons passed, 0 failed, 1 MTP-only case skipped, 60 correlated decision events, and 61 redacted validation rows. Covered cases were deterministic generation, system-prompt boundary, mid-conversation, after tool output, near-context long prefix, and branch/retry cache reuse. |

## What Is Not Proven Yet

- This is not production distributed-cache correctness.
- Logit/top-k restore validation remains required; live/distributed restore
  negative coverage beyond offline strict-key and blob fixtures remains
  required.
- MTP-enabled restore correctness, MTP-enabled suffix-route/TTFT evidence,
  restart/two-node restore, and distributed correctness remain separate from
  the no-MTP live passes summarized above.
- The current implementation is designed for trusted private LANs, not the
  public internet.
- The router has not been hardened for quotas, adversarial tenants, production
  eviction/GC, concurrent load, or untrusted networks.
- Model files, runtime flags, tokenizer/template state, KV types, and cache
  compatibility settings must match for safe restore.

## Publication Rule

Use the table above for public claims. Keep raw run folders in private evidence
storage unless they have been explicitly redacted and reviewed.
