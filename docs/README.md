# Documentation

Start with the root `README.md` for the project overview. This directory keeps
setup guides, endpoint documentation, architecture notes, retained evidence
policy, and claim mapping.

## Recommended Reader Path

1. `architecture/cache-router-setup.md`: generic worker inventory and live setup.
2. `architecture/cache-router-openai-endpoint.md`: endpoint behavior and cache
   request extension.
3. `architecture/cache-router.md`: router architecture.
4. `benchmark-claim-map.md`: what the retained evidence supports.
5. `raw-evidence-retention-audit.md`: evidence and privacy rules.
6. `provenance.md`: how private lab evidence is redacted before publication.

## Document Classes

- `architecture/`: design notes, setup guides, endpoint docs, and example
  contract fixtures.
- `architecture/examples/`: small replayable contract fixtures used by offline
  validation.
- `benchmark-claim-map.md`: public wording guardrail for performance and cache
  claims.
- `raw-evidence-retention-audit.md`: rules for what may be tracked or
  published.
- `provenance.md`: publication-safe source and evidence provenance guidance.

Historical notes and raw lab evidence should stay outside this public repo
unless they have been redacted into a small summary. New setup instructions
should use generic worker and router names, placeholder LAN addresses, and local
deployment filenames.
