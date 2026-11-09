# Batch jobs: the job engine

The job engine runs one job over many devices and returns one checked result. You say what to compute over which input; the engine cuts the input into partitions, gives each partition to a computer or browser tab that may and can run it, retries what gets lost, combines the accepted partial results, and checks the combination before it calls the job done.

It is on the `feat/job-engine` branch (stacked on `feat/agent-tasks`). The older compute jobs (`harness run primes ...`, `pool.map`) still work as before; agent tasks are described in [agent-tasks.md](agent-tasks.md). Every field of a job is in [job-spec.md](job-spec.md); the trust rules are in [security.md](security.md); setting up each kind of device is in [devices.md](devices.md); what the blueprint asked for and what is built is in [blueprint-status.md](blueprint-status.md).

## What you can run

| Workload | What it does | Runs on |
| --- | --- | --- |
| `batch.aggregate` | Count, sum, mean, min and max over records, with filters | Your computers, friends' computers (shareable data), browser tabs |
| `batch.groupby` | The same per group; `shuffle: true` spreads very many groups over computers in buckets | The same (browser tabs: without shuffle) |
| `batch.transform` | Filter, select, rename, rolling mean; writes records | Your computers and friends' computers |
| `batch.join` | Join two datasets on key fields (inner or left), through hashed buckets | Your computers and friends' computers |
| `python.map` | Your own function over items, a parameter grid, or a dataset's records | Your computers only |
| `ml.evaluate` | Score a model (Python or TorchScript) on labeled records: accuracy, per-class precision/recall/F1, confusion matrix | Your computers only |
| `tests.pytest` | A pytest suite in shards (`harness test`) | Your computers only |

Datasets are JSON Lines, CSV or text files. Each is indexed once when added, so partitions can start at any record boundary. A job of up to 8 **stages** passes records from one stage to the next (`spec.stages`); each stage starts only after the one before it is complete and checked.

## The pieces

- **Job**: the normalized JobSpec, its status (`running`, `reducing`, `verifying`, `done`, `failed`, `cancelled`, `needs_approval`) and its result. Jobs, stages, partitions and attempts live in SQLite on the coordinator (`<data dir>/engine/engine.db`), so a coordinator restart loses nothing: running jobs carry on when it is back.
- **Partition**: a fixed piece of the input (a byte range of a dataset, a slice of items, an earlier stage's output file, or a bucket). Its id never changes, and coverage is checked: the partitions of a stage read every byte, item or bucket exactly once.
- **Attempt**: one try at a partition on one device, with a token of its own. The token reads only that partition's input and writes only that attempt's outputs, and it stops working when the attempt ends. Exactly one attempt per partition is accepted; a late result from an attempt that was replaced is ignored.
- **Lease**: a running attempt sends a heartbeat every 2 s; a partition whose lease (45 s by default) runs out goes back to the queue. Resumable operators also send checkpoints, so a retry continues where the last attempt stopped.
- **Artifacts**: files stored on the coordinator by SHA-256 (datasets, outputs, models, project archives, stage contexts). Every download is checked against its digest.

## Where a job runs

`spec.mode` is `automatic` (default), `local` (one computer) or `distributed`. In automatic mode the planner estimates both: the time on the fastest single computer against the time spread over every eligible one, including sending the input over the network (25 MB/s, or `HARNESS_NETWORK_BPS`). It uses each computer's measured throughput from earlier jobs when there is any. Small jobs usually stay on one computer: starting a worker and fetching its input costs about 0.4 s per partition, and the plan says so.

A device takes partitions only when it is connected, sharing, has a free slot, can run the workload and read the format, is allowed by the trust rules ([security.md](security.md)), and has the runtime the job needs. The plan lists every device with the reason it can't take part, for example "is a friend's computer, and the dataset isn't marked shareable" or "has no container engine ready (Docker or Podman, running) for isolated-oci".

Computers pull partitions when they have room. A partition prefers the computer that already holds its input (an earlier stage's output it wrote or fetched) and waits up to 3 s for it to be free. A partition of a side-effect-free job that runs at least 3 times the stage's median and at least 10 s may also run on a free computer; the first result wins (`retry.speculate`, on by default; at most a tenth of a stage at once).

Agent tasks and batch partitions share one resource ledger, so together they don't ask more of a computer than its cores and free memory.

## Getting the right answer

- **Exact arithmetic.** Sums are kept as integers counting units of 2^-1074, so a total is the same however the input was split and in whatever order the pieces came back. Means, minimums and maximums follow from exact states. Integers stay integers.
- **Typed merges.** Each workload has its own reducer; a group-by merges groups by key, a test job merges outcomes by test id and refuses a test that appears twice.
- **Validation label.** Every finished job says how its result was checked: `structural` (every partition accepted once, coverage complete, states well formed) or `spot-checked`. With `output.spotCheck: N`, N partitions are computed again and must match byte for byte: by the coordinator itself for the built-in operators, or on another of your computers for your own code (a "verify" attempt). The result stays hidden until they match; a mismatch fails the job with "Spot check failed" and publishes nothing.
- **Provenance.** A result names exactly what it came from: the dataset's SHA-256, the model's or project's SHA-256, the code's SHA-256, the context version, and the runtime (Python environment id, or container image digest).

## When things go wrong

- A device that disconnects or stops answering loses its partitions when the lease runs out. A device you **remove** loses them at once.
- A failed attempt is retried up to `retry.maxAttempts` (3) times; a partition that keeps losing its computer fails the job after 12 moves. A partition that runs longer than `retry.timeoutSeconds` (900) is stopped and retried.
- A record that doesn't parse fails the partition, or is skipped and counted with `output.onBadRecord: "skip"`.
- When a computer reconnects it reports what it is running and what it finished; the coordinator cancels what it no longer wants and accepts results it hadn't received.
- A browser tab that is hidden hands its partitions back at once. That isn't a failed try; the job goes to others for 30 s.

## Runtimes for your own code

| `spec.runtime.kind` | What it is | Setup |
| --- | --- | --- |
| `native-python` (default) | The Python the agent runs on, with its packages | None |
| `managed-python` | An environment with exactly the Python version and pinned packages the job names, made with uv | Approved per job and computer; downloads (uv, Python, packages) only when you allow them |
| `isolated-oci` | A container image named by its digest, with Docker or Podman: no network, read-only system files, not root, CPU/memory/process limits | Approved per job and computer; the image is pulled only when you allow downloads, then checked |

A job whose runtime no computer has yet waits with status `needs_approval` and shows exactly what each computer would run (`uv venv ...`, `uv pip install ...`, `docker pull ...`, and the check). Approve it in the Batch app, with `harness job approve ID [--downloads] [--device NAME]`, or `harness job run JOB.json --approve-setup [--downloads]`. Once a computer has an environment or a checked image, later jobs that ask for the same one start at once. A failed setup says why; nothing half-built is kept, and nothing falls back to running outside the environment or container.

In a container, the computer downloads the partition's input first and uploads its outputs after the container ends, so the code never has network access and never sees the attempt's token.

## Browser tabs

A tab that shares its processor takes partitions of `batch.aggregate` and `batch.groupby` over JSON Lines, CSV and text, computed in a Web Worker by a JavaScript port of the engine's operators (`web/engine-ops.js`). It must give the coordinator exactly the state a computer would, so integers are BigInts, sums are exact, and states travel as text. Where JavaScript can't be sure to match Python (regular expressions, digits from other scripts, group keys holding objects with numeric keys, NUL characters in CSV, comparing objects with text) the tab reports "unsupported" and a computer takes the partition; that isn't a failed try.

Before offering anything the tab measures itself once: a short benchmark, its cores and memory, its battery, and whether it has what the workers need. It offers no partitions while the page is hidden, while sharing is off, or on a battery under 20% that isn't charging. The Batch app's Computers tab shows each tab's measurement and why it isn't taking work.

## The desktop app

While its agent works for the pool, the desktop app asks the system not to suspend (Electron `powerSaveBlocker`, "prevent app suspension"), so a partition or agent task survives the screen locking. The screen itself may still turn off. By default it does this only on mains power; the tray menu has "Keep it awake while it works, on battery too" and "Let it sleep as usual".

## Using it

**The Batch app** (in the web page): build a job from a form (totals, group-bys, joins, transforms, your function, a model evaluation), see the plan before starting, follow each job's partitions live, approve setups, download results, add datasets and models, and see what each computer can run.

**The command line** (`harness`, signed in as an agent computer or inside the workspace):

```bash
harness dataset add sales.jsonl                 # or --format csv, --shareable
harness job plan job.json                       # where it would run and why
harness job run job.json                        # start it, wait, print the result
harness job ls | show ID | wait ID | result ID -o out.jsonl | cancel ID | retry ID
harness model add model.zip                     # model.py with predict(model, batch); or a TorchScript .pt
harness test ./myproject -- -k "not slow"       # a pytest suite in shards (below)
```

**Python** (`yourharness.jobs`):

```python
from yourharness import jobs

ds = jobs.dataset("sales.jsonl")
jobs.aggregate(ds, sum="amount", mean="amount", where=jobs.where("region", "==", "north"))
jobs.group_by(ds, "region", sum="amount")
jobs.map_items(score, range(1000))                                   # your function on your computers
jobs.map_items(score, items, python="3.12", requirements=["numpy==2.1.3"], approve_setup=True, downloads=True)
jobs.map_items(score, items, container="python:3.12-slim@sha256:...", limits={"memoryMB": 1024}, approve_setup=True)
jobs.evaluate(jobs.model("model.zip"), ds, label="label")
```

## Test suites: `harness test`

`harness test [PROJECT] [-- PYTEST OPTIONS]` collects the test ids on the computer you're on, with `--rootdir` set to the project so every computer names each test the same way. It packs the files git tracks into a .zip, uploads it with its SHA-256, and starts a `tests.pytest` job.

The job cuts the tests into shards of about equal running time: longest first onto the emptiest shard, using how long each test took last time (kept in `~/.yourharness/test-durations/`), about 20 s per shard unless `--shards` says. Every test id lands in exactly one shard. Each computer unpacks the archive, copies it fresh, and runs pytest on just that shard's tests: a small plugin deselects the rest and records each test's outcome. Failing tests run again in a fresh copy (`--reruns`, default 1). A test that then passes is reported as **flaky**, never as passed. A test pytest didn't find is **missing**; a test in a file that didn't import is an **error**.

It prints the counts and each test that didn't pass, writes a JUnit XML report with `--junit FILE`, and exits 0 only when nothing failed, errored or went missing (2 when it waits for a setup approval). `--spot-check N` runs N shards again on another computer and compares outcomes; a test that changes outcome between computers fails the check. The computers need pytest and the project's packages in the Python the job uses: the agent's own by default, a managed environment (`--python 3.12 -r pytest==8.3.3 -r ...`), or an image (`--container IMAGE@sha256:...`).

Coverage data isn't merged yet.

## Try it

On the coordinator's computer (it joins its own pool), with one more computer joined:

1. `harness dataset add sales.jsonl` (any JSON Lines or CSV file), then in the Batch app make a Totals job over it with mode "Spread over computers". Both computers appear under the job; the result shows `structural` or, with spot checks, `spot-checked`.
2. Open the page on a phone (joined with your code), keep it visible, and run a group-by over a dataset of a few MB with mode "Spread over computers": the phone takes partitions, and the Batch app's Computers tab shows its measured speed. Lock the phone mid-job: its partitions go back at once and the job still finishes.
3. `harness test .` in a project with a pytest suite, on two computers that have pytest.
4. On a computer with Docker running, run a `python.map` job with `"runtime": {"kind": "isolated-oci", "image": "python:3.12-slim@sha256:..."}` (get the digest with `docker image inspect --format '{{index .RepoDigests 0}}' python:3.12-slim`). Approve it; the code inside can't reach the network.
5. Remove a friend's computer in the Devices list while it works: its partitions move at once, its agent deletes what it kept for the pool, and **Activity** still shows what it did.

## Benchmarks

`python bench/run.py --label "Mac + PC over Wi-Fi"` times the same jobs kept on one computer and spread over the pool, and saves the numbers under `bench/results/`. On one Mac, tiny jobs are slower through the engine (about 0.6 s of overhead); the point of the numbers is to show when spreading pays, not to claim it always does.
