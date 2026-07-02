# Cache Router Configs

Use `workers.example.json` as the public template for new deployments. Copy it
to a local file, replace every placeholder, and add one row per worker.

Do not commit local deployment inventories unless they are sanitized examples.
Real inventories often include LAN addresses, SSH aliases, usernames, model
paths, and cache roots that are specific to one home network.

Recommended pattern:

```bash
cp configs/cache-router/workers.example.json configs/cache-router/<name>.local.json
```

Then edit `<name>.local.json` for your machines and keep it out of Git if it
contains private topology.

Files matching `configs/cache-router/*.local.json` are ignored by default.
