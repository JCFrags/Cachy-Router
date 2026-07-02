# Second-Node Cache Validation Plan

/goal

This plan is current when a maintainer can add a second AMD Strix Halo machine
and run cross-node cache validation without inventing results, weakening tenant
isolation, or treating offline router fixtures as distributed-cache evidence.

## Status

Plan only. Only one AMD Strix Halo host is currently available: `nimo-1`. Do not
claim two-node cache restore, distributed cache routing, failover, or multi-node
performance until a second equivalent host exists and this plan passes.

## Required Node Parity

Before any cross-node test, record both nodes in a private operator note and a
sanitized tracked summary:

| area | required parity evidence |
|---|---|
| hardware | CPU/APU model, memory size, firmware/BIOS-relevant settings, iGPU allocation, storage tier |
| OS/runtime | CachyOS version, kernel, Vulkan/RADV or backend driver, compiler/runtime libraries |
| `llama.cpp` | source commit, build flags, binary path, cache ABI, backend features |
| model files | same GGUF shards, MTP/draft model, hashes, tensor manifest hash, tokenizer hash |
| service profile | context size, KV types, checkpoint config, flash attention, MTP/speculative flags |
| security | tenant namespace policy, raw artifact directory, encryption-at-rest plan, cleanup owner |
| network | private link, transfer path, object-store/shared-filesystem path, expected bandwidth |

Any mismatch becomes a test variable. It must not be hidden inside a "same
machine" claim.

Minimum parity fields for the sanitized summary:

```text
node_id
CPU/APU model
RAM size and iGPU allocation
BIOS/firmware-relevant settings
CachyOS version and kernel
Vulkan/RADV version or alternate backend driver
llama.cpp commit and build flags
runtime binary path summary
service profile hash
main GGUF shard hashes
MTP/draft model hash
tokenizer and chat-template hashes
network path and durable-store path summary
```

## Allowed Claims By Phase

| phase | allowed claim after pass | still not allowed |
|---|---|---|
| parity preflight | nodes appear compatible for an isolated test | cache restore works |
| transfer smoke | cache blob can be copied/checksummed between nodes | restored KV is correct |
| correctness pass | one prompt family restored correctly across nodes | production router or general workload claims |
| negative controls | strict-key mismatches fail closed | cross-tenant reuse is safe by default |
| performance probe | TTFT/restore latency measured for that prompt/profile | general speedup without repeat runs and cold controls |

## Cross-Node Sequence

1. Confirm both nodes are isolated from production traffic.
2. Record node parity metadata and strict cache key fields.
3. Start approved workers on Node A and Node B with matching profiles.
4. Run a cold baseline on Node B for the prompt family.
5. Run the same prompt prefix on Node A and save a cache/slot artifact.
6. Write artifact to a temp path, fsync, hash, and verify byte size.
7. Upload or copy the immutable blob to the durable/shared store.
8. Hydrate the blob to Node B local NVMe through the sidecar or scripted stand-in.
9. Verify checksum on Node B before restore.
10. Restore into a fresh Node B slot/session.
11. Compare next-token logits or top-k distribution against Node B cold prefill.
12. Compare deterministic continuation text.
13. Measure restore latency, TTFT, prompt processing speed, and eval speed.
14. Emit decision and validation events.
15. Run negative controls.
16. Clean up temporary workers, slot files, local blobs, and transfer scratch.

## Correctness Cases

Minimum prompt families:

- operator-controlled system prompt only;
- system prompt plus short user turn;
- mid-conversation restore;
- restore after tool-output-shaped content;
- branch/retry shared-prefix restore;
- near-context-limit restore within approved memory bounds;
- MTP-enabled restore;
- MTP-disabled restore, if an approved comparable profile exists.

## Negative And Failure Tests

| case | required result |
|---|---|
| corrupt blob | checksum failure, quarantine, no restore |
| truncated blob | checksum/size failure, quarantine, no restore |
| wrong model hash | miss before restore |
| wrong tokenizer/template | miss before restore |
| wrong `llama.cpp` commit or cache ABI | miss before restore |
| wrong backend or driver lane | miss before restore |
| wrong KV/cache type | miss before restore |
| wrong MTP draft model/config | miss before restore |
| wrong tenant | policy denial and audit event |
| missing durable blob | cold-prefill fallback |
| stale local residency | registry correction plus hydrate or cold prefill |
| Node B restore API failure | mark manifest suspect and cold-prefill fallback |

## Metrics

Record:

- cold TTFT and restored TTFT;
- restore latency and hydration latency;
- prompt tokens, cached tokens, processed prompt tokens, and generated tokens;
- prompt processing speed and eval speed;
- cache lookup latency and routing decision latency;
- local NVMe hit rate and durable-cache hit rate;
- fallback count, quarantine count, and restore-failure count;
- memory pressure and OOM/near-OOM events;
- full-context reprocess indicators;
- erased/discarded cache indicators.

## Security And Tenancy

KV cache blobs are sensitive data. For the first second-node test:

- cross-tenant reuse is denied;
- `global_system` cache is allowed only for operator-approved synthetic prefixes;
- raw prompts and tenant identifiers are not stored in public artifacts;
- tenant deletion and blob GC are tested before any publication claim;
- cache-hit probing is treated as a side-channel risk;
- durable/shared storage uses encryption at rest before private data is involved.

## Evidence Paths

Future tracked outputs should be small and sanitized:

```text
data/cache_primitives/<date>-second-node-cache-validation/README.md
data/cache_primitives/<date>-second-node-cache-validation/metadata.json
data/cache_primitives/<date>-second-node-cache-validation/manifest.jsonl
data/cache_primitives/<date>-second-node-cache-validation/validation_results.jsonl
data/cache_primitives/<date>-second-node-cache-validation/decision_events.jsonl
data/cache_primitives/<date>-second-node-cache-validation/summary.tsv
```

Raw blobs, slot files, transfer scratch, full logs, and node-local inventories
must remain ignored and outside public release artifacts.

Expected curated artifacts:

| artifact | purpose |
|---|---|
| node parity summary | proves which hardware/runtime fields matched and which were variables |
| cache manifest | strict key and blob metadata published only after checksum verification |
| blob checksum record | source hash, destination hash, size, and transfer verification |
| restore validation result | pass/fail/error, tolerance, fallback/quarantine decision, and worker ids |
| next-token logits/top-k comparison | correctness comparison against Node B cold prefill |
| deterministic text comparison | fixed-sampling restored continuation versus Node B cold continuation |
| cold versus restored timing table | TTFT, restore latency, hydration latency, prompt processing, eval speed |
| decision events | router/sidecar-equivalent lookup, hydrate, restore, fallback, or quarantine attribution |
| cleanup proof | temporary workers stopped and raw artifacts absent from tracked release index |

## Stop Conditions

Stop before or during execution if:

- node parity cannot be proven;
- either service is not isolated from production traffic;
- memory headroom falls below the approved threshold;
- model/runtime metadata mismatches unexpectedly;
- checksum or validation fails repeatedly;
- raw private prompts or credentials appear in logs;
- cleanup cannot be guaranteed;
- the test would require publishing, uploading, sudo, or persistent service
  mutation.
