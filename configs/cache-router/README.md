# Cache Router Configs

Use `workers.example.json` as the public template for new deployments. Copy it
to a local file, replace the topology placeholders, and add one row per worker.
The example enables `strict_metadata_auto` and
`strict_metadata_force_runtime`, so the daemon derives strict cache-key metadata
from worker runtime APIs at startup and replaces stale hand-entered strict
fields before it builds cache keys. Keep explicit strict cache-key fields only
when you intentionally want to pin or audit an operator-supplied value; disable
`strict_metadata_force_runtime` for those pinned deployments.

The top-level `cache_storage` block declares the router-owned durable blob
store. For cache build/use, place that cache root on an operator-managed
encrypted cache root and replace
`durable_blob_encryption_at_rest.volume_id_hash` with a non-secret SHA-256
digest of the encrypted volume identifier. Do not store raw volume names,
encryption keys, credentials, or token files in the inventory.

Do not commit local deployment inventories unless they are sanitized examples.
Real inventories often include LAN addresses, SSH aliases, usernames, model
paths, and cache roots that are specific to one home network.

Recommended pattern:

```bash
cp configs/cache-router/workers.example.json configs/cache-router/<name>.local.json
```

Then edit `<name>.local.json` for your machines and keep it out of Git if it
contains private topology.

Files matching `configs/cache-router/*.local.json`,
`configs/cache-router/local*.json`, and
`configs/cache-router/*.workers.json` are ignored by default.
