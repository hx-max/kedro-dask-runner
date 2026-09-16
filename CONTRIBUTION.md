# DaskRunner — run a Kedro pipeline on a Dask cluster

This fork of [`kedro-org/kedro`](https://github.com/kedro-org/kedro) adds one runner:
`DaskRunner`, an `AbstractRunner` implementation that schedules the pipeline on a
[Dask](https://www.dask.org/) cluster.

Everything else in this repository is upstream, unchanged.

## Why a Dask runner

`SequentialRunner` runs one node at a time, `ThreadRunner` uses a thread pool,
`ParallelRunner` forks processes. None of them gives you a scheduler with a live
dashboard, retries, or a path from your laptop to a cluster of machines. Dask does,
and the pipeline does not have to change to use it.

## What was added

| File | Change |
|---|---|
| `kedro/runner/dask_runner.py` | new, `DaskRunner` |
| `kedro/runner/__init__.py` | export `DaskRunner` |
| `tests/runner/test_dask_runner.py` | new, 36 tests |
| `RELEASE.md` | changelog entry |
| `demo_dask_runner.py` | new, runnable demonstration |

## Usage

```python
from kedro.runner import DaskRunner

# local cluster, in-memory datasets work
DaskRunner(n_workers=4, threads_per_worker=1).run(pipeline, catalog)

# or bring your own cluster
DaskRunner(client=distributed.Client("tcp://scheduler:8786")).run(pipeline, catalog)
```

The constructor takes `client`, `n_workers`, `threads_per_worker`, `processes`,
`is_async`, `retries`, and forwards anything else to `LocalCluster`. A `client` that
is passed in is reused and left running; a cluster the runner starts itself is closed
when the run ends.

## The design problem, and what it forced

The obvious implementation — submit the Kedro `Task` to Dask with
`client.submit(task)` — produces a pipeline that runs to completion and then returns
wrong results, with no exception until the end:

```
kedro.io.core.DatasetError: Data for MemoryDataset has not been saved yet.
```

Two failures came out of the same root cause. **Dask pickles every task it is handed,
even when the workers are threads of the current process**, so each task carried a
*fresh copy* of the whole `DataCatalog`:

1. A dataset saved by one node was written into a copy, invisible to the node that
   consumed it.
2. `MemoryDataset` marks "no data yet" with a module level sentinel, `_data is _EMPTY`.
   Unpickling re-creates the sentinel as a new `object()`, so the identity check stops
   matching and an *unsaved* dataset cheerfully returns that object instead of raising
   — which is how a node ended up computing `object * int`.

`DaskRunner` therefore works in two modes, chosen by whether the cluster's workers
share the runner's memory (`client.cluster.processes`):

- **Same process (the default threaded cluster).** The tasks stay in a process local
  registry and Dask is handed nothing but their coordinates. No data is copied, no
  sentinel is broken, and any pipeline that runs under `ThreadRunner` runs here — with
  Dask's scheduler underneath.
- **Another process (a `processes=True` cluster or a remote scheduler).** The task has
  to travel, so the catalog travels with it, and this is only possible when the data
  lives outside the process. The runner validates that up front: non-serialisable
  datasets and nodes raise `AttributeError`, and in-memory datasets that link nodes
  raise `ValueError` naming them, instead of failing halfway through the run.

Datasets are released as soon as their last consumer has run, the dependency
bookkeeping is the same as in `AbstractRunner._run`, and the pipeline is never handed
to Dask as one opaque graph — so `--from-nodes` resume hints and dataset release keep
working.

## Verify it

```bash
pip install -e . "dask[distributed]" pytest pytest-mock
python -m pytest tests/runner/test_dask_runner.py -q -o addopts=""   # 36 passed
python demo_dask_runner.py
```

The demo runs one pipeline with both runners:

```
1. Both runners agree
  SequentialRunner : pi = 3.14190
  DaskRunner       : pi = 3.14190

2. Overlapping 8 nodes that each wait 1.0s
  SequentialRunner :   8.25s  (waits are paid one after the other)
  DaskRunner       :   1.20s  (8 workers on 12 CPUs)
  speedup          :   6.88x
```

The waiting nodes are what a cluster is for: the run costs the slowest node, not the
sum of the nodes. A threaded cluster does nothing for CPU bound Python code, because
the GIL serialises it — that is what `processes=True` or a remote client is for.

## Test coverage

`tests/runner/test_dask_runner.py` covers running pipelines with in-memory and
catalog-provided inputs, hook manager propagation, injected clients not being closed,
the runner's own cluster being created from the constructor arguments and closed
afterwards, `client_kwargs` forwarding, empty pipelines, dependency order, failure
propagation, worker count capping, parameter validation, and the whole validation
layer for clusters whose workers run in another process.

## Note on the distributed path

The `processes=True` path needs a Kedro project to be configured, exactly like
`ParallelRunner`, because node hooks are rebuilt inside the worker. It was verified up
to and including validation against a real process-based cluster; the full execution
was not exercised here, since this machine cannot spawn the worker processes.
