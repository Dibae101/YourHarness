"""The job engine: one job, many devices, one checked result.

A job (``spec.py``) says what to compute over which input, and how partial results
combine. The input is cut into partitions with fixed ids and exact ownership
(``partition.py``); each partition runs on one computer (``worker.py``) through a
built-in operator or your own code (``ops.py``); the accepted partial results are
combined by a typed reducer (``reducer.py``) into one result, the same however the
input was split and in whatever order the pieces finished.

The modules a worker needs (exact, records, ops, partition, reducer, worker) use the
standard library only, so they also run inside a managed Python environment that
has nothing but your job's own packages.
"""
