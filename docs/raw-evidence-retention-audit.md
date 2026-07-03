# Raw Evidence Retention Audit

This audit is current when a maintainer can tell which evidence is tracked,
which evidence must stay ignored locally, which evidence may become a curated
release asset later, and which evidence must never be public.

## Current Scan Snapshot

Date: 2026-07-03.

Commands are local and do not contact private hosts. The public hygiene scanner
prints rule names and locations only; it does not print matched secret values.

| check | result |
|---|---:|
| public hygiene scanner | 0 findings across 75 public paths |
| tracked ignored file scan | 0 files |
| public evidence size/text scan | passed |
| high-confidence secret value scan | 0 value hits |
| private topology scan | 0 value hits |
| contract fixture privacy validation | passed |

The scanner allows explicit placeholders such as `<router-lan-ip>`,
`<worker-a-lan-ip>`, `/home/<user>/...`, and test-only `secret-token` values,
but rejects concrete private hostnames, private LAN addresses, local user paths,
private-key headers, and real-looking API tokens in publishable files.

## Evidence Categories

| category | examples | Git policy | release/public policy |
|---|---|---|---|
| tracked curated summaries | README notes, metadata JSON, manifest JSONL, summary TSV, benchmark tables, schemas, docs, small reviewed result JSONL | allowed | suitable for public source package when claim tier is clear |
| ignored local raw evidence | raw server logs, raw SSH captures, `/tmp` endpoint dumps, generated official-suite `run-*` dirs, scratch/tmp dirs | ignored | reduce to curated summaries before any publication |
| suitable future release assets after review | freshly built package archives, curated benchmark bundles, small reviewed public prompt fixtures | not automatic | owner-approved only after strict public-handoff audit |
| never public | `.env*`, credentials, provider configs, auth state, private keys, browser state, raw Hermes homes, private workspaces, full transcripts, model/checkpoint blobs, raw slot files, cache blobs, SSD cache dirs | ignored or rejected | never include in Git, release index, package archives, benchmark bundles, or public posts |

## Required Ignore Coverage

The current ignore and release-index policy covers:

- generated official-suite result directories: `official_test_suite/results/run-*`;
- local release archives: `dist/`;
- raw Hermes homes and state: `data/**/hermes_home/`, `data/**/raw_hermes_home/`;
- private/raw workspaces: `data/**/raw_workspaces/`, `data/**/private_workspaces/`;
- raw logs and transcripts: `data/**/raw_logs/`, `data/**/transcripts/`, `data/**/full_transcripts/`, `*.transcript.jsonl`;
- cache/slot artifacts: `data/cache_primitives/**/raw_slots/`, `raw_cache/`, `cache_blobs/`, `ssd_cache/`, `slot_files/`;
- environment and credential-like files: `.env*`, auth, credential, token, provider config, private-key, npm/pypi config, and netrc patterns;
- model/checkpoint artifacts: GGUF, safetensors, PyTorch, ONNX, checkpoint, binary, and SQLite journal blobs.

## Representative Ignore Proof

The audit checked representative paths for these classes:

```text
data/mtp_experiments/example/raw_logs/server.log
data/mtp_experiments/example/transcripts/full.transcript.jsonl
data/cache_primitives/example/raw_slots/slot.bin
data/cache_primitives/example/cache_blobs/blob.bin
data/cache_primitives/example/slot_files/slot.bin
data/anything/hermes_home/state.db
step37-hermes-pilot-workspaces/example/README.md
dist/example.tar.gz
.env
model.gguf
```

All were ignored by the expected `.gitignore` rules.

## Review Rule

Before publishing, packaging, or promoting a new result:

1. Track only curated summaries and small reviewed data.
2. Run the release index and release audit.
3. Run the unsafe indexed-path scan.
4. Keep raw artifacts outside Git or in ignored paths.
5. Do not rewrite historical raw evidence merely to hide old local paths; use
   processed views and `docs/path-portability-audit.md` for public readers.
