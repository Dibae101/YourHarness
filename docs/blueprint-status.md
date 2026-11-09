# Blueprint status

What the *Complete Architecture and Implementation Blueprint* (which reviewed commit `763e4c4`) asked for, and what the `feat/job-engine` branch has. **Done** means built and covered by automated tests; **Partial** says what's missing; **Not built** is left for later. Nothing here has been tested on physical devices other than one Mac, and no AI agent CLI has been run (by request); those rows say so.

Commits: Step 0 `52c43b3`, 1 `07e1c6e`, 2 `1487fb8`, 3 `c3d44d5`, 4 `3bb68e1`, 5 `157a601`, 6 `84034fa`, 7 `873830a`, 8 `c2b95fa`, 9 `dea2cc0`, 10 `e55841b`, 11 `ac77b9c` (and the macOS fix `b552380`), then the docs.

## P0: baseline and safety

| ID | Item | Status |
| --- | --- | --- |
| P0-01 | Test matrix and commit | Done: CI on Ubuntu and macOS, Python 3.10 and 3.12, plus the web smoke tests |
| P0-02 | Physical Mac + Windows/WSL join | Not verified here: needs your devices (the steps are in the README) |
| P0-03 | Two-command coding PR scenario | Done against a fake GitHub (`tests/test_agent_tasks.py`); a real repository is your morning test |
| P0-04 | Restrict third-party native execution | Done: friends' devices never get code, custom JS, pool shell commands or file access; agent tasks there are edit-only unless the friend allows commands |
| P0-05 | Versioned wire and manifest design | Partial: `apiVersion: yourharness.dev/v1alpha1`, numbered database migrations, fixtures in the contract tests; no compatibility suite across releases |
| P0-06 | Benchmarks and trace events | Partial: `bench/run.py`, per-job event log, per-computer throughput history; no two-device baseline yet |

## P1: the engine

| ID | Item | Status |
| --- | --- | --- |
| P1-01 | JobSpec validation and adapters | Done (`engine/spec.py`, [job-spec.md](job-spec.md)) |
| P1-02 | SQLite jobs, stages, partitions, attempts | Done; a restart keeps every job (`tests/test_engine_e2e.py`) |
| P1-03 | Streaming ingest | Partial: JSON Lines, CSV and text stream in with a split index; no Arrow or Parquet |
| P1-04 | Stable partitioner with exact ownership | Done: byte ranges on record boundaries, items, artifacts, buckets; coverage checked at every combine |
| P1-05 | Resource filter and reservations | Done: one ledger for compute pieces, partitions and agent tasks |
| P1-06 | Output manifests and typed reducers | Done: exact sums (integer units of 2^-1074), typed merges, outputs by SHA-256 |
| P1-07 | Leases, fencing, retries, checkpoints | Done: per-attempt tokens, one accepted attempt per partition, resumable checkpoints |
| P1-08 | Job details UI and export | Done: the Batch app, result downloads |
| P1-09 | Baseline performance study | Partial: the benchmark exists and was run on one Mac (tiny jobs pay about 0.6 s of overhead); two-device numbers need your devices |

## P2: environments

| ID | Item | Status |
| --- | --- | --- |
| P2-01 | Runtime fingerprint and cache | Done |
| P2-02 | uv and managed Python | Partial: uv is downloaded on demand with its checksum checked, not bundled in the desktop apps |
| P2-03 | GPU probe and preflight | Partial: GPUs are reported (Metal, CUDA) and each environment's check lists what torch sees; no validated GPU jobs |
| P2-04 | Setup plan and approval | Done: exact commands per computer, downloads opt-in, in the UI, CLI and SDK |
| P2-05 | Execution under the runtime manager | Done: nothing touches the host's Python |
| P2-06 | Error recovery and eviction | Done: failed setups leave nothing; least recently used environments go first (10 GB) |
| P2-07 | Warm second run | Done: a second job with the same environment starts at once |

## P3: ML evaluation and test sharding

| ID | Item | Status |
| --- | --- | --- |
| P3-01 | Model artifact and provenance | Done: models pinned by SHA-256, every result names model and data digests |
| P3-02 | CPU evaluation adapter | Done: Python models (`predict(model, batch)`) tested; TorchScript adapter written, tested only where torch is installed |
| P3-03 | Global metrics reducer | Done: exact confusion counts, so metrics don't depend on the split |
| P3-04 | Test discovery and shard partitioner | Done: `harness test`, duration-balanced shards, every test id exactly once |
| P3-05 | JUnit and coverage aggregation | Partial: JUnit report, flaky detection; coverage isn't merged |
| P3-06 | GPU evaluation matrix | Not built |
| P3-07 | Real two-device ML and test benchmarks | Not built: needs your devices |

## P4: context, DAGs and agent runs

| ID | Item | Status |
| --- | --- | --- |
| P4-01 | Immutable context snapshots | Done: by SHA-256, per stage |
| P4-02 | DAG stages and barriers | Done: up to 8 sequential stages; each starts after the one before is checked |
| P4-03 | Halo partitions and keyed shuffle | Done: halos for rolling windows, hashed buckets for group-bys and joins |
| P4-04 | Locality and benchmark-based placement | Done: data held where it was made, measured throughput, a short wait for the holder; speculative stragglers |
| P4-05 | Agent tasks share reservations | Done |
| P4-06 | Trusted final tests on the merged branch | Done: run on the coordinator's computer; the PR says Validated or Not validated; optional gate |
| P4-07 | Real Claude/Codex/Kiro smoke matrix | Partial: `scripts/ai_smoke_matrix.py` is written and was only run with `--dry-run`, which starts no AI CLI. Run it with `--yes` when you want to spend the tokens |
| P4-08 | Semantic index | Not built (optional) |

## P5: browsers, phones and power

| ID | Item | Status |
| --- | --- | --- |
| P5-01 | Browser profiler and benchmark | Done: measured once, unknowns stay unknown |
| P5-02 | Safe browser job registry | Done: friends' tabs get only the built-in operators; owner-written JS stays owner-only |
| P5-03 | Approved JS/WASM operators | Done for JavaScript (totals, group-bys), byte-identical to Python or handed to a computer; no WASM |
| P5-04 | Foreground and visibility | Done: a hidden tab hands its partitions back at once. Not yet tried on a locked iPhone or Android phone |
| P5-05 | Desktop power management | Done: the app holds off suspension while it works (mains power by default). Not yet tried with the screen locked on your Mac and PC |
| P5-06 | Restart recovery | Done: coordinator restarts and computers reconnecting reconcile what runs |
| P5-07 | HTTPS, Wake Lock, WebGPU preflight | Partial: HTTPS (Tailscale or `--tls-cert`), the page's Wake Lock; no WebGPU jobs |
| P5-08 | Small-model browser inference | Not built |

## P6: isolation and friends

| ID | Item | Status |
| --- | --- | --- |
| P6-01 | Isolation policy enforcement | Done: `engine/isolation.py`, enforced by the coordinator and the agent; no fallback |
| P6-02 | Hardened OCI/VM backend | Done for containers (Docker or Podman; checked per image and limits; real-container test on the Linux CI runners); no VM backend |
| P6-03 | Private networking and scoped tokens | Partial: per-attempt tokens, HTTPS with your certificate or Tailscale; no built-in private network |
| P6-04 | Revocation, cleanup, consent | Done for revocation and cleanup (immediate hand-back, friend clean-up, activity on both sides, audit trail); per-job consent prompts on the friend's side aren't built |
| P6-05 | Approved third-party code workloads | Not built, on purpose: friends' computers never run a job's code |
| P6-06 | GPU isolated workloads | Not built |
| P6-07 | Direct peer transfer | Not built: everything goes through the coordinator |

## P7: later

| ID | Item | Status |
| --- | --- | --- |
| P7-01 | Hyperparameter trials | Partial: `python.map` over a parameter grid with the `best` reducer; no checkpoint selection |
| P7-02 to P7-07 | DDP, federated training, model sharding, automated planning, signed installers and SBOM, hosted coordinator | Not built |
