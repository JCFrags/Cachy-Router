# Provenance

Cachy Router was extracted from a private trusted-LAN research workspace. The
standalone source tree keeps reusable code, schemas, generic examples, and
redacted summaries. Private deployment details stay out of the public repo.

Do not commit:

- hostnames, LAN IPs, usernames, or absolute home-directory paths from a private
  deployment;
- raw runtime logs, PID files, auth token files, or environment captures;
- slot files, cache blobs, model files, or full prompts.

When documenting a live result, publish a short summary with:

- hardware class and runtime version;
- model family and quant;
- relevant `llama-server` flags;
- prompt size and measurement method;
- reduction or latency numbers;
- caveats and missing controls.

Keep the raw evidence in private storage unless it has been reviewed and
redacted.
