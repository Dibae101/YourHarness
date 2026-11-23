# JobSpec reference

A batch job is a JSON (or YAML, with PyYAML installed) document. The coordinator checks it and fills in every default before anything runs; a field that's wrong is named in the error, for example `spec.runtime.image: should be a container image pinned by its digest`. `harness job plan JOB.json` checks one and shows where it would run. How jobs run is in [engine.md](engine.md).

```json
{
  "apiVersion": "yourharness.dev/v1alpha1",
  "kind": "Job",
  "metadata": {"name": "Sales by region", "labels": {"team": "me"}},
  "spec": {
    "mode": "automatic",
    "workload": {"type": "batch.groupby", "by": ["region"], "metrics": [{"op": "sum", "field": "amount"}]},
    "inputs": {"dataset": {"ref": "dataset:d1a2b3c4"}}
  }
}
```

`apiVersion` and `kind` may be left out. `metadata.name` is at most 120 characters. The whole document is at most 2 MB.

## spec.mode

`automatic` (default: whichever is expected to finish sooner), `local` (every partition on one computer), or `distributed`.

## spec.workload

Field names may use dots to reach into nested objects (`customer.city`).

**Filters** (`where`, a list): `{"field": F, "op": OP, "value": V}` with `==`, `!=`, `<`, `<=`, `>`, `>=` (numbers), `contains` (text), `regex` (at most 500 characters; browser tabs leave these to computers), `in` (a list), or `exists` / `missing` (no value). At most 50.

**Metrics** (`metrics`, a list): `{"op": "count"}` or `{"op": "sum" | "mean" | "min" | "max", "field": F}`, with an optional `"as": NAME` (default `sum_amount` and so on). At most 50, each with its own name.

| `type` | Fields |
| --- | --- |
| `batch.aggregate` | `metrics` (default count), `where` |
| `batch.groupby` | `by` (1 to 50 fields), `metrics`, `where`, `shuffle` (`true` or `{"buckets": 1-1024}`: for over about 200,000 groups; the groups are written as records) |
| `batch.transform` | `where`, `select` (fields to keep), `rename` (`{"old": "new"}`), `rolling` (`{"field": F, "window": 1-10000, "as": NAME}`, a rolling mean; reads only the job's inputs) |
| `batch.join` | `on` (key fields), `rightOn` (default: `on`), `how` (`inner` or `left`), `buckets` (1-1024). The second dataset is `inputs.right`. Keys that read as numbers match as numbers; a right field whose name the left record has comes out as `right_<name>` |
| `python.map` | `code` (Python source, at most 200 KB, defining the function), `function` (default `process`; takes the item, or the item and the context), `reducer` (`collect` default, `concat` writes records, `sum`, `count`, `merge_counts`, `best`), `bestBy` (`{"key": K, "order": "max" \| "min"}` for `best`), `batch` (items per call, default 1), `sideEffects` (`none` default, or `external`: never run twice at once) |
| `ml.evaluate` | `label` (default `label`), `features` (fields given to the model; default all but label and id), `id`, `batch` (default 64), `predictions` (also write every prediction as a record), `labels` (the model's output order, up to 10,000), `device` (`cpu` default, or `auto`). The model is `inputs.model` |
| `tests.pytest` | `tests` (the collected test ids, 1 to 20,000, no duplicates), `durations` (`{test id: seconds}` from an earlier run), `args` (pytest options, at most 50; not `-p`, `--rootdir`, `--junitxml`, `-c`, `--collect-only`), `shards` (1-256; default about 20 s of tests each), `reruns` (0-3, default 1), `timeout` (seconds per shard run, default 3600). The project is `inputs.project`. `harness test` writes this for you |

A normalized test job keeps `count` instead of the list, and its `inputs.items` holds the shards: each test id in exactly one.

## spec.inputs

| Field | For |
| --- | --- |
| `dataset: {"ref": "dataset:d..."}` | every dataset workload |
| `right: {"ref": "dataset:d..."}` | `batch.join` |
| `items: [...]` (1 to 100,000 JSON values) or `grid: {"lr": [0.1, 0.01], "depth": [2, 4]}` (every combination) | `python.map` |
| `model: {"ref": "model:m...", "sha256": ...}` | `ml.evaluate`; the SHA-256 is filled in when the job is created, so the job always uses exactly that file |
| `project: {"ref": "model:m...", "sha256": ...}` | `tests.pytest`; a .zip uploaded with adapter `project` |

## spec.stages

Up to 8 stages, run one after another: `[{"name": "clean", "workload": {...}}, {"name": "totals", "workload": {...}, "from": "clean"}]`. Names are lowercase letters, digits and dashes. `from` is `inputs` (default) or an earlier stage that writes records (`batch.transform`, `python.map` with `concat`, `ml.evaluate` with `predictions`, a shuffled group-by). A join, a shuffled group-by and a test job run as jobs of their own. The last stage's result is the job's result. Each stage starts only when the one before it is complete and checked.

## spec.context

`{"values": {...}, "version": "<64 hex digits>"}`: JSON values every call of your function receives as its second argument (`def process(item, ctx)`), at most 1 MB. The version is the SHA-256 of the values written with sorted keys; give it to make sure the job uses exactly those values (the coordinator refuses values that don't match it). Each stage reads the snapshot by its digest.

## spec.runtime

Only for your own code (`python.map`, `ml.evaluate`, `tests.pytest`); the built-in operators always run with the engine's own.

| Field | Meaning |
| --- | --- |
| `kind` | `native-python` (default), `managed-python`, or `isolated-oci` |
| `python` | managed-python: the version, like `"3.12"` |
| `requirements` | managed-python: package requirements, a package name first (no pip options such as `--index-url`), at most 200 |
| `image` | isolated-oci: an image pinned by digest, `name@sha256:<64 hex digits>`. A tag alone is refused |
| `limits` | isolated-oci: `cpus` (0.25-64, default 1), `memoryMB` (128-262144, default 2048), `pids` (32-4096, default 256) |
| `network` | isolated-oci: only `"none"` |
| `isolation` | `owned-trusted` or `isolated`; `isolated` requires `isolated-oci` (native and managed Python aren't sandboxes) |

## spec.partitioning

| Field | Default | Meaning |
| --- | --- | --- |
| `targetBytes` | from the plan | Bytes per partition (1 KB to 2 GB) |
| `targetDurationSeconds` | 30 | What the plan aims a partition at |
| `maxPartitions` | 5000 | Per stage |
| `maxInflightPerWorker` | 4 | |
| `itemsPerPartition` | from the plan | For items; always 1 shard for a test job |

## spec.resources

`cpuSlots` (1-64, default 1) and `minFreeMemoryGiB` (default 0.25): a partition goes only where at least that much is free, after what running work has reserved.

## spec.security

`allowedTrust`: `owned-only` (default) or `owned-and-friends`. Friends' computers and friends' browser tabs then take the built-in operators' partitions of datasets marked shareable. A job with your own code can't allow friends, even in a container. `allowUntrustedNativeFallback` can't be turned on.

## spec.retry

| Field | Default | Meaning |
| --- | --- | --- |
| `maxAttempts` | 3 | Tries per partition (1-20) |
| `timeoutSeconds` | 900 | A try running longer is stopped and retried |
| `leaseSeconds` | 45 | A partition whose computer is silent this long goes back to the queue |
| `checkpointIntervalSeconds` | 10 | How often resumable operators save their place |
| `speculate` | true | A straggler of a side-effect-free job may also run on a free computer; the first result wins |

## spec.output

| Field | Default | Meaning |
| --- | --- | --- |
| `onBadRecord` | `fail` | Or `skip`: count bad records and carry on (the first 20 are listed) |
| `spotCheck` | 0 | Partitions computed again (by the coordinator, or on another of your computers for your own code) that must match exactly |
| `retentionDays` | 7 | How long the job's output files are kept |
