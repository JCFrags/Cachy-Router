# Source Tree

This document describes the standalone Cachy Router source tree.

```text
configs/cache-router/  worker inventory template and local config guidance
docs/                 setup, endpoint, architecture, and evidence policy
evidence/             redacted public summaries only
schemas/cache-router/ JSON schemas for router contracts and validation results
scripts/              router daemon, worker sidecar, stack helpers, validators
```

The repository intentionally does not include raw proof artifacts, model files,
slot files, cache blobs, remote runtime homes, private deployment inventories,
or generated package archives.

Start with the root `README.md`, then read:

1. `docs/architecture/cache-router-setup.md`
2. `docs/architecture/cache-router-openai-endpoint.md`
3. `docs/architecture/cache-router.md`
4. `evidence/cache-router-results-summary.md`
5. `docs/raw-evidence-retention-audit.md`
