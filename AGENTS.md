# Agent Instructions

Treat this directory as the root of a future standalone public GitHub
repository for Cachy Router.

## Scope

- Edit only inside `Cachy-Router/` unless the user explicitly widens scope.
- Do not run remote commands unless the user explicitly asks for live runtime
  work.
- Keep raw run artifacts out of the public tree. Publish only redacted summaries
  under `evidence/` after review.
- New public setup docs should use generic worker/router names, placeholder LAN
  addresses, and local deployment filenames.

## Product Framing

- Cachy Router is a trusted-LAN OpenAI-compatible router for `llama.cpp`
  workers.
- The router may run on any machine that can reach the worker URLs and sidecars,
  including a worker, controller, desktop, or independent aggregator.
- The inventory is N-worker. Do not hardcode or imply a two-worker limit.
- The default MVP mode is unauthenticated on a trusted LAN. Make the security
  boundary explicit whenever documenting `0.0.0.0` or unauthenticated sidecars,
  workers, or router endpoints.
- Prefer Python standard library only unless a dependency is explicitly
  justified.

## Evidence And Claims

Separate these claim classes:

- normal OpenAI-compatible pass-through;
- explicit suffix-route cache hits;
- worker-local hot cache;
- router-owned durable blob hydration;
- scheduling or busy-worker routing probes;
- correctness tests that are still missing.

Do not claim production distributed-cache correctness until restore negatives
and logit/top-k or deterministic-output validation pass.

## Hygiene

Do not add model files, cache blobs, slot files, raw logs, raw prompts, remote
runtime homes, credentials, token files, `.env` files, or generated archives.

Useful offline checks:

```bash
make check
python3 scripts/cache_router_setup_doctor.py --workers-file configs/cache-router/workers.example.json
python3 scripts/validate_cache_router_contracts.py --json
python3 scripts/cache_router_offline_prototype.py --json
```

Live checks may contact private hosts. Use them only when the operator asks for
live runtime work, and use an operator-supplied deployment inventory:

```bash
python3 scripts/cache_router_remote_stack.py workers-status \
  --workers-file configs/cache-router/<deployment>.workers.json
```
