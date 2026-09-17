# DaskRunner for Kedro

> **An upstream snapshot, not a standalone project.**
>
> Everything in this repository except one file is [`kedro-org/kedro`](https://github.com/kedro-org/kedro)
> at **v1.6.0**, unmodified — the same `kedro/`, `tests/` and `docs/` tree.
> The one addition is `kedro/runner/dask_runner.py`.
>
> If you are looking for Kedro you want [kedro-org/kedro](https://github.com/kedro-org/kedro),
> not this repository. This is a working snapshot of a change that is being proposed
> back to Kedro.

`DaskRunner` is an `AbstractRunner` implementation that executes the nodes of a Kedro pipeline
on a [Dask](https://www.dask.org/) cluster, respecting the inter-node dependencies.

## Upstream status

| Item | State |
| --- | --- |
| [PR #5776](https://github.com/kedro-org/kedro/pull/5776) — make the `MemoryDataset` empty sentinel survive pickling | open |
| [Issue #5773](https://github.com/kedro-org/kedro/issues/5773) — the sentinel bug, with a reproduction | open |
| [Issue #5774](https://github.com/kedro-org/kedro/issues/5774) — the Dask deployment guide's runner recipe does not run against Kedro 1.6.0 | open |

Kedro's `CONTRIBUTING.md` asks for a change to be discussed in an issue before a pull request is
opened, so the issues came first.

## What was added

| File | Change |
| --- | --- |
| `kedro/runner/dask_runner.py` | new — `DaskRunner` |
| `kedro/runner/__init__.py` | exports `DaskRunner` |
| `tests/runner/test_dask_runner.py` | new — 36 tests |
| `RELEASE.md` | changelog entry |
| `demo_dask_runner.py` | runnable demonstration |
| `CONTRIBUTION.md` | what was added, and what was not |

Nothing else differs from Kedro v1.6.0.

## Why this was not just "submit the task to Dask"

The obvious implementation — `client.submit(task)` — runs to completion and then returns *wrong
results*, with no exception until the end. Dask pickles every task it is handed, **even when the
workers are threads of the current process**, so each task carried its own copy of the
`DataCatalog`: a dataset written by one node was invisible to the node that consumed it.

`DaskRunner` therefore works in two modes, chosen by whether the workers share the runner's
memory:

- **Same process (the default threaded cluster).** Tasks stay in a process-local registry and Dask
  is handed nothing but their coordinates. Any pipeline that runs under `ThreadRunner` runs here,
  with Dask's scheduler underneath.
- **Another process (`processes=True`, or a remote scheduler).** The catalogue has to travel with
  the task, which is only possible when the data lives outside the process. That is validated up
  front instead of failing halfway through the run.

## What the demo shows

`demo_dask_runner.py` runs the same pipeline twice: eight chunks that each wait one second, then
combine.

| Runner | Wall clock |
| --- | --- |
| `SequentialRunner` | 8.25 s — the waits are paid one after the other |
| `DaskRunner` | 1.20 s — eight workers |

**6.9×**, for the same result. The point is not the number: the demo is embarrassingly parallel by
construction, which is exactly what a cluster is good at. The run costs the slowest node, not the
sum of the nodes. A threaded cluster does nothing for CPU-bound Python, because the GIL serialises
it — that is what `processes=True` or a remote client is for.

## Honest limits

- The Dask deployment guide's own recipe is the starting point here, and **that recipe does not run
  against Kedro 1.6.0**. Reported as issue [#5774](https://github.com/kedro-org/kedro/issues/5774).
- `DaskRunner(processes=True)` and connecting to a remote cluster are implemented but were **not**
  exercised end to end — that needs more than one machine. Validation against a real process-based
  cluster was verified; full execution was not.
- Which is itself a bug: in-memory datasets do not survive that serialisation cleanly. Reported as
  issue [#5773](https://github.com/kedro-org/kedro/issues/5773) and fixed by
  PR [#5776](https://github.com/kedro-org/kedro/pull/5776).

## Licence

Kedro is distributed under the **Apache 2.0** licence. This snapshot carries the same licence — see
[`LICENSE.md`](./LICENSE.md), unchanged from upstream.

## Where this is going

The same change is proposed to Kedro itself. Once it lands there, this snapshot should be archived
in favour of the upstream repository.
