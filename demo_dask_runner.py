"""Run one Kedro pipeline with two runners: ``SequentialRunner`` and ``DaskRunner``.

The pipeline estimates pi by Monte Carlo, in eight independent chunks, then combines
the chunks. The maths is not the point; *who runs it* is.

Two experiments:

1. **The result does not change.** The same pipeline object, the same catalog, two
   runners, one answer.
2. **Waiting gets overlapped.** With a cluster, the eight independent nodes run at the
   same time and the total wall clock is the slowest node rather than the sum of the
   nodes. That is what a cluster buys you for nodes that wait on an API, a database or
   a file, and it is what this runner is for.

Run it:

    python demo_dask_runner.py
"""

import os
import time
from time import perf_counter

import numpy as np
from kedro.io import DataCatalog, MemoryDataset
from kedro.pipeline import node, pipeline
from kedro.runner import DaskRunner, SequentialRunner

N_CHUNKS = 8
WAIT_PER_NODE = 1.0


def sample_chunk(seed: int, n_samples: int, delay: float = 0.0) -> tuple[int, int]:
    """Count `n_samples` random points that fall inside the unit circle.

    `delay` stands in for a node that waits on something external. Real pipelines are
    full of those: a query, a download, a model call.
    """
    if delay:
        time.sleep(delay)

    rng = np.random.default_rng(seed)
    x = rng.random(n_samples)
    y = rng.random(n_samples)
    inside = int(np.count_nonzero(x * x + y * y <= 1.0))
    return inside, n_samples


def combine(*partials: tuple[int, int]) -> float:
    """Turn the counts of every chunk into one estimate of pi."""
    inside = sum(count for count, _ in partials)
    total = sum(size for _, size in partials)
    return 4.0 * inside / total


def build_pipeline():
    """One chunk per node, then a node that combines them."""
    return pipeline(
        [
            *[
                node(
                    sample_chunk,
                    inputs={
                        "seed": f"params:seed_{i}",
                        "n_samples": "params:n_samples",
                        "delay": "params:delay",
                    },
                    outputs=f"partial_{i}",
                    name=f"chunk_{i}",
                )
                for i in range(N_CHUNKS)
            ],
            node(
                combine,
                inputs=[f"partial_{i}" for i in range(N_CHUNKS)],
                outputs="pi_estimate",
                name="combine",
            ),
        ]
    )


def build_catalog(n_samples: int, delay: float) -> DataCatalog:
    catalog = DataCatalog()
    for i in range(N_CHUNKS):
        catalog[f"params:seed_{i}"] = MemoryDataset(i)
    catalog["params:n_samples"] = MemoryDataset(n_samples)
    catalog["params:delay"] = MemoryDataset(delay)
    return catalog


def main() -> None:
    from distributed import Client, LocalCluster

    the_pipeline = build_pipeline()

    print("=" * 78)
    print("1. Both runners agree")
    print("=" * 78)
    sequential_pi = SequentialRunner().run(
        the_pipeline, build_catalog(n_samples=1_000_000, delay=0.0)
    )["pi_estimate"].load()

    cluster = LocalCluster(
        n_workers=N_CHUNKS,
        threads_per_worker=1,
        processes=False,  # threads share this process, so in-memory datasets survive
        dashboard_address=None,
    )
    client = Client(cluster)
    try:
        dask_pi = DaskRunner(client=client).run(
            the_pipeline, build_catalog(n_samples=1_000_000, delay=0.0)
        )["pi_estimate"].load()

        print(f"\n  SequentialRunner : pi = {sequential_pi:.5f}")
        print(f"  DaskRunner       : pi = {dask_pi:.5f}")

        print("\n" + "=" * 78)
        print(f"2. Overlapping {N_CHUNKS} nodes that each wait {WAIT_PER_NODE}s")
        print("=" * 78 + "\n")

        start = perf_counter()
        SequentialRunner().run(
            the_pipeline, build_catalog(n_samples=100_000, delay=WAIT_PER_NODE)
        )
        sequential_seconds = perf_counter() - start

        start = perf_counter()
        DaskRunner(client=client).run(
            the_pipeline, build_catalog(n_samples=100_000, delay=WAIT_PER_NODE)
        )
        dask_seconds = perf_counter() - start
    finally:
        client.close()
        cluster.close()

    print(f"  SequentialRunner : {sequential_seconds:6.2f}s  "
          f"(waits are paid one after the other)")
    print(f"  DaskRunner       : {dask_seconds:6.2f}s  "
          f"({N_CHUNKS} workers on {os.cpu_count()} CPUs)")
    print(f"  speedup          : {sequential_seconds / dask_seconds:6.2f}x")

    print(
        "\nThe nodes here wait rather than compute, which is what a cluster is good at:\n"
        "the run costs the slowest node, not the sum of the nodes. A threaded cluster\n"
        "does nothing for CPU bound Python code, because the GIL serialises it; that is\n"
        "what ``processes=True`` or a remote client is for, and those need a catalog\n"
        "whose datasets are persisted rather than in memory."
    )


if __name__ == "__main__":
    main()
