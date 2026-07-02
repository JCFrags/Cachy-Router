# Claim Map

Use this map before turning cache-router experiments into README text, release
notes, blog posts, or benchmark tables.

## Claim Tiers

| tier | meaning |
|---|---|
| Portable behavior | Expected from the code path itself and covered by offline checks. |
| Exploratory live result | Measured in a bounded trusted-LAN experiment; useful, but not production proof. |
| Architecture plan | Designed and documented, but not fully implemented or validated. |
| Future work | Planned but not yet tested. |
| Not claimed | Must not be presented as working behavior. |

## Current Public Claims

| claim | tier | acceptable wording | evidence |
|---|---|---|---|
| The router exposes an OpenAI-compatible surface for normal completion and chat pass-through. | Portable behavior | "Cachy Router provides OpenAI-style `/v1/completions`, `/v1/chat/completions`, and `/v1/models` endpoints for trusted-LAN deployments." | `scripts/cache_router_daemon.py`, `docs/architecture/cache-router-openai-endpoint.md` |
| Worker inventories are not limited to two machines. | Portable behavior | "The inventory format accepts one row per worker; two workers are only an example." | `configs/cache-router/workers.example.json`, `scripts/cache_router_make_inventory.py` |
| The explicit cache extension can build, restore, and use prefix caches through suffix-only routing. | Exploratory live result | "Bounded experiments showed large prompt-processing reductions when a restored prefix was followed by a suffix-only route." | `evidence/cache-router-results-summary.md`, `scripts/cache_router_one_node_poc.py`, `scripts/cache_router_store_hydration_poc.py` |
| Router-owned durable blobs plus worker-local hot copies are the intended cache ownership model. | Exploratory live result | "The prototype demonstrated router-owned blob storage and worker-local hydration for compatible workers." | `evidence/cache-router-results-summary.md`, `docs/architecture/cache-router.md` |
| The router may run on a worker or an independent aggregator machine. | Portable behavior | "Run the router on any trusted-LAN host that can reach worker URLs and sidecars." | `README.md`, `docs/architecture/cache-router-setup.md` |
| The project is ready for untrusted networks or public internet exposure. | Not claimed | Do not claim this. | Requires authentication, authorization, tenant isolation, rate limits, TLS, hardening, and adversarial testing. |
| Distributed cache correctness is proven for arbitrary models and runtimes. | Not claimed | Do not claim this. | Requires strict-key negatives, logit/top-k or deterministic restore checks, model hash validation, backend compatibility checks, and repeated tests. |
| Cachy Router is production load-balancing infrastructure. | Not claimed | Do not claim this. | Requires queueing policy, concurrency tests, eviction, durability policy, monitoring, and operational packaging. |

## Required Caveats

- Cache restore is only safe for compatible model, tokenizer, template, runtime,
  backend, KV type, context, and speculative-decoding settings.
- The accelerated path is explicit: build or restore a prefix, then send only
  the suffix/new work. Full-prompt replay may still reprocess the prompt.
- No-key mode is for trusted private LANs only.
- Raw proof artifacts should stay private unless they are redacted into a small
  public summary.
