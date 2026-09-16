"""``DaskRunner`` is an ``AbstractRunner`` implementation. It can be used to run
the ``Pipeline`` on a `Dask <https://www.dask.org/>`_ cluster, so that node
execution is scheduled by Dask instead of the ``concurrent.futures`` executors
used by the other runners.
"""

from __future__ import annotations

import os
from collections import Counter
from itertools import chain
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from kedro.runner.runner import AbstractRunner
from kedro.runner.task import Task

if TYPE_CHECKING:
    from collections.abc import Iterable

    from distributed import Client, Future
    from pluggy import PluginManager

    from kedro.io import CatalogProtocol
    from kedro.pipeline import Pipeline
    from kedro.pipeline.node import Node


#: Tasks of the runs whose workers live in the current process, addressed by run key
#: and submission order. See ``_execute_registered_task`` for why they are not sent.
_TASK_REGISTRY: dict[tuple[str, int], Task] = {}


def _execute_registered_task(run_key: str, index: int) -> None:
    """Run a task that the runner left in ``_TASK_REGISTRY``.

    Dask pickles every task handed to it, even when the workers are threads of the
    current process, and a pickled ``Task`` drags a copy of the whole catalog with it.
    Copies break in-memory pipelines twice over: the datasets saved by one node are
    invisible to the next one, and ``MemoryDataset``'s "no data yet" sentinel stops
    being recognised after a round trip through pickle, so unsaved datasets look like
    saved ones. Shipping nothing but the coordinates of the task sidesteps both.
    """
    try:
        task = _TASK_REGISTRY.pop((run_key, index))
    except KeyError:  # pragma: no cover - only reachable if a run is torn down
        raise RuntimeError(
            f"Task {index} of run '{run_key}' is not registered in this process. It "
            f"was probably scheduled on a worker that does not share the runner's "
            f"memory, which in-process tasks require."
        ) from None
    task.execute()


class DaskRunner(AbstractRunner):
    """``DaskRunner`` is an ``AbstractRunner`` implementation that executes the
    ``Pipeline`` on a Dask cluster.

    Dask brings three things the other runners do not have: a scheduler that keeps
    track of every task, a live dashboard, and the ability to move the very same
    pipeline from a local cluster to a remote one without touching the code.

    By default the runner creates a **local threaded cluster** and closes it at the
    end of the run. In that configuration the workers live in the same process as the
    runner, so "in memory" datasets (the ones Kedro creates implicitly for every node
    output that is not in the catalog) are shared exactly as they are with
    ``ThreadRunner``, and any pipeline runs unchanged::

        DaskRunner().run(pipeline, catalog)

    "Shared exactly" is worth spelling out: Dask pickles every task it is given, even
    when the workers are threads of the current process, and a pickled catalog is a
    *copy*. The dataset saved by one node would then be invisible to the next one, so
    ``DaskRunner`` keeps the tasks of an in-process cluster in a registry and hands
    Dask nothing but their coordinates. The data never leaves the process.

    Set ``processes=True`` (or pass an existing ``client`` connected to a remote
    scheduler) to spread the work over several processes or machines. Workers in
    another process cannot see the runner's memory, so in that configuration:

    * every dataset that is not a pipeline input **must be persisted**, that is,
      declared in the catalog with a file based or database dataset. In-memory
      datasets are rejected with a ``ValueError`` instead of failing later, when the
      data would silently be missing.
    * datasets must be serialisable, which is validated up front the same way
      ``ParallelRunner`` does it, and each task carries the catalog to its worker.
    * nodes must be importable by the workers, and node hooks run inside the worker,
      exactly as with ``ParallelRunner``.

    Unlike ``ParallelRunner``, ``DaskRunner`` needs no ``multiprocessing`` manager,
    because in the distributed case the datasets are persisted rather than shared.
    Datasets are released as soon as their last consumer has run, which keeps the
    memory footprint of a long pipeline flat.

    Example:
    ```python
        from kedro.runner import DaskRunner

        # local Dask cluster with 4 workers of 1 thread each
        runner = DaskRunner(n_workers=4, threads_per_worker=1)
        runner.run(pipeline, catalog)
    ```
    """

    def __init__(
        self,
        client: Client | None = None,
        n_workers: int | None = None,
        threads_per_worker: int | None = None,
        processes: bool = False,
        is_async: bool = False,
        retries: int = 0,
        **client_kwargs: Any,
    ):
        """
        Instantiates the runner.

        Args:
            client: A ``dask.distributed.Client`` to submit the tasks to. When
                provided it is used as is, and it is **not** closed at the end of the
                run, so the same client can serve several runs. When ``None`` the
                runner creates a local cluster of its own and closes it afterwards.
            n_workers: Number of Dask workers to start when the runner creates its own
                cluster. Defaults to the number of CPUs.
            threads_per_worker: Number of threads of each local worker. Defaults to 1.
            processes: Whether the local workers should run in separate processes
                instead of threads. Defaults to ``False``, because processes cannot
                see the runner's memory and therefore require a fully persistent
                catalog.
            is_async: If True, the inputs and outputs of each node are loaded and
                saved asynchronously with threads, inside the worker. Defaults to
                False.
            retries: How many times Dask should retry a failed task before giving up.
                Defaults to 0, that is, no retries.
            **client_kwargs: Any other argument is forwarded to ``LocalCluster``, for
                example ``memory_limit`` or ``dashboard_address``.
        Raises:
            ImportError: If ``dask[distributed]`` is not installed.
            ValueError: If a parameter is not valid.
        """
        _require_distributed()

        super().__init__(is_async=is_async)

        _check_positive_int("n_workers", n_workers)
        _check_positive_int("threads_per_worker", threads_per_worker)

        if not isinstance(processes, bool):
            raise ValueError(
                f"processes takes only booleans True and False. Got {processes} instead."
            )

        if not isinstance(retries, int) or isinstance(retries, bool) or retries < 0:
            raise ValueError("retries should be a non-negative integer")

        self.client = client
        self.n_workers = n_workers
        self.threads_per_worker = threads_per_worker
        self.processes = processes
        self.retries = retries
        self.client_kwargs = client_kwargs

        # set by _run, so that the validation methods know which kind of cluster they
        # are validating for
        self._active_client: Client | None = None

    # -- cluster management -------------------------------------------------

    def _create_client(self) -> Client:
        """Start a local Dask cluster and return a client connected to it."""
        from distributed import Client, LocalCluster

        cluster = LocalCluster(
            n_workers=self.n_workers or os.cpu_count() or 1,
            threads_per_worker=self.threads_per_worker or 1,
            processes=self.processes,
            **self.client_kwargs,
        )
        client = Client(cluster)
        dashboard = getattr(client, "dashboard_link", None)
        if dashboard:
            self._logger.info("Dask dashboard available at %s", dashboard)
        return client

    def _is_shared_memory_client(self) -> bool:
        """Whether the workers of the active cluster live in the runner's process.

        Only a local cluster running threads can share Python objects, and therefore
        Kedro's in-memory datasets, with the runner. A client connected to a remote
        scheduler always answers ``False``: it is impossible to tell whether its
        workers share memory with us, and the safe assumption is that they do not.
        """
        cluster = getattr(self._active_client, "cluster", None)
        processes = getattr(cluster, "processes", None)
        if processes is None:
            return False
        return not processes

    # -- validation ---------------------------------------------------------

    def _validate_catalog(self, catalog: CatalogProtocol) -> None:
        """Check that the workers can be handed the datasets they must load and save.

        Only enforced when the workers run in another process: a threaded cluster
        uses the very same dataset objects, so nothing is ever sent anywhere.
        """
        if self._is_shared_memory_client():
            return

        import cloudpickle

        unserialisable = []
        for name, dataset in getattr(catalog, "_datasets", {}).items():
            if getattr(dataset, "_SINGLE_PROCESS", False):
                unserialisable.append(name)
                continue
            try:
                cloudpickle.dumps(dataset)
            except Exception:
                unserialisable.append(name)

        if unserialisable:
            raise AttributeError(
                f"The following datasets cannot be serialised and therefore cannot be "
                f"sent to the Dask workers: {sorted(unserialisable)}"
            )

    def _validate_nodes(self, nodes: Iterable[Node]) -> None:
        """Check that the workers can be handed the nodes.

        Only enforced for clusters whose workers run in another process: a threaded
        local cluster never serialises anything, and rejecting lambdas and closures
        there would make ``DaskRunner`` stricter than ``ThreadRunner`` for no reason.
        """
        if self._is_shared_memory_client():
            return

        import cloudpickle

        unserialisable = []
        for node in nodes:
            try:
                cloudpickle.dumps(node)
            except Exception:
                unserialisable.append(node)

        if unserialisable:
            raise AttributeError(
                f"The following nodes cannot be serialised: {sorted(unserialisable)}\n"
                f"In order to run them on a Dask cluster, the workers need to be able "
                f"to import them, i.e. nodes should not include lambda functions, "
                f"nested functions, closures, etc."
            )

    def _validate_shared_memory_datasets(
        self, pipeline: Pipeline, catalog: CatalogProtocol
    ) -> None:
        """Reject pipelines that rely on the runner's memory to move data around.

        Every dataset of the pipeline that is not an input either links two nodes or
        holds a final result. In both cases a worker running in another process would
        keep the data to itself: the next node would not find its input, and the
        caller of ``run`` would not find the pipeline outputs. Failing here is much
        clearer than an "Data for MemoryDataset has not been saved yet" error that
        appears to contradict the fact that the node did run.
        """
        if self._is_shared_memory_client():
            return

        ephemeral = sorted(
            name
            for name in pipeline.datasets() - set(pipeline.inputs())
            if _is_ephemeral(name, catalog)
        )
        if not ephemeral:
            return

        raise ValueError(
            f"The following datasets only exist in memory: {ephemeral}.\nA Dask "
            f"cluster whose workers run in separate processes cannot share them, so "
            f"declare them in the catalog as file based or database datasets, or run "
            f"DaskRunner with its default local threaded cluster."
        )

    # -- scheduling ---------------------------------------------------------

    def _get_required_workers_count(self, pipeline: Pipeline) -> int:
        """Number of workers that can usefully run the pipeline in parallel."""
        required_workers = len(pipeline.nodes) - len(pipeline.grouped_nodes) + 1
        if self.n_workers is None:
            return required_workers
        return min(required_workers, self.n_workers)

    def _get_executor(self, max_workers: int) -> None:
        """Unused: the Dask scheduler replaces the ``concurrent.futures`` executors.

        ``AbstractRunner._run`` cannot be reused here because Dask futures are not
        ``concurrent.futures.Future`` objects and cannot be awaited with
        ``concurrent.futures.wait``, so ``DaskRunner`` implements its own ``_run``.
        """
        return None

    def _run(
        self,
        pipeline: Pipeline,
        catalog: CatalogProtocol,
        hook_manager: PluginManager | None = None,
        run_id: str | None = None,
    ) -> None:
        """The method implementing pipeline running on a Dask cluster.

        Args:
            pipeline: The ``Pipeline`` to run.
            catalog: An implemented instance of ``CatalogProtocol`` from which to fetch
                data.
            hook_manager: The ``PluginManager`` to activate hooks.
            run_id: The id of the run.

        Raises:
            ValueError: When the catalog or the pipeline is not compatible with a
                cluster whose workers run in another process.
            AttributeError: When a node cannot be serialised.
            RuntimeError: If the runner is unable to schedule the execution of all
                pipeline nodes.
            Exception: In case of any downstream node failure.
        """
        owns_client = self.client is None
        client = self.client or self._create_client()
        self._active_client = client
        try:
            self._validate_catalog(catalog)
            self._validate_nodes(pipeline.nodes)
            self._validate_shared_memory_datasets(pipeline, catalog)
            self._execute(client, pipeline, catalog, hook_manager, run_id)
        finally:
            self._active_client = None
            if owns_client:
                cluster = getattr(client, "cluster", None)
                client.close()
                if cluster is not None:
                    # closing the client does not stop the cluster it was given
                    cluster.close()

    def _execute(
        self,
        client: Client,
        pipeline: Pipeline,
        catalog: CatalogProtocol,
        hook_manager: PluginManager | None,
        run_id: str | None,
    ) -> None:
        """Submit the nodes as they become runnable, in topological order.

        The dependency bookkeeping is the same as in ``AbstractRunner._run``: only the
        nodes whose dependencies have all completed are submitted. The pipeline is
        therefore never handed to Dask as one opaque graph, which is what allows
        intermediate datasets to be released as soon as they are no longer needed.
        """
        from distributed import wait as dask_wait

        nodes = pipeline.nodes
        distributed_cluster = not self._is_shared_memory_client()
        run_key = uuid4().hex
        registered: list[tuple[str, int]] = []

        load_counts = Counter(chain.from_iterable(n.inputs for n in nodes))
        node_dependencies = pipeline.node_dependencies
        todo_nodes = set(node_dependencies.keys())
        done_nodes: set[Node] = set()
        futures: dict[Future, Node] = {}
        done: set[Future] = set()
        task_index = 0

        try:
            while True:
                ready = {n for n in todo_nodes if node_dependencies[n] <= done_nodes}
                todo_nodes -= ready
                for node in ready:
                    task = Task(
                        node=node,
                        catalog=catalog,
                        hook_manager=hook_manager,
                        is_async=self._is_async,
                        run_id=run_id,
                        # a worker in another process cannot reuse our hook manager
                        parallel=distributed_cluster,
                    )
                    key = f"kedro::{run_id or 'run'}::{task_index}::{node.name}"
                    if distributed_cluster:
                        # the worker needs the catalog, so the task travels with it
                        future = client.submit(task, key=key, retries=self.retries)
                    else:
                        # the workers share our memory, so keep the task here
                        _TASK_REGISTRY[(run_key, task_index)] = task
                        registered.append((run_key, task_index))
                        future = client.submit(
                            _execute_registered_task,
                            run_key,
                            task_index,
                            key=key,
                            retries=self.retries,
                        )
                    futures[future] = node
                    task_index += 1

                if not futures:
                    if todo_nodes:
                        self._raise_runtime_error(todo_nodes, done_nodes, ready, done)
                    break

                done, _ = dask_wait(set(futures), return_when="FIRST_COMPLETED")
                for future in done:
                    node = futures.pop(future)
                    try:
                        future.result()
                    except Exception:
                        self._suggest_resume_scenario(pipeline, done_nodes, catalog)
                        raise
                    done_nodes.add(node)
                    self._logger.info("Completed node: %s", node.name)
                    self._logger.info(
                        "Completed %d out of %d tasks", len(done_nodes), len(nodes)
                    )
                    self._release_datasets(node, catalog, load_counts, pipeline)
        finally:
            # tasks that never ran, because the run failed, must not be leaked
            for registry_key in registered:
                _TASK_REGISTRY.pop(registry_key, None)


def _require_distributed() -> None:
    """Raise a helpful error when ``dask.distributed`` is not installed."""
    try:
        import distributed  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "DaskRunner requires the optional dependency 'dask[distributed]'. "
            "Install it with `pip install 'dask[distributed]'`."
        ) from exc


def _check_positive_int(name: str, value: Any) -> None:
    """Validate an optional strictly positive integer parameter."""
    if value is None:
        return
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} should be a positive integer")


def _is_ephemeral(dataset_name: str, catalog: CatalogProtocol) -> bool:
    """Whether a dataset lives only in memory, so it cannot cross process boundaries.

    A dataset that is not in the catalog is a ``MemoryDataset`` created by Kedro on
    the fly, and any dataset declaring ``_EPHEMERAL = True`` (``MemoryDataset``,
    ``CachedDataset``, ...) has the same problem. Parameters are excluded because
    ``params:x`` is resolved by the workers from their own configuration.
    """
    if dataset_name.startswith("params:"):
        return False
    if dataset_name not in catalog:
        return True
    dataset = catalog.get(dataset_name)
    return getattr(dataset, "_EPHEMERAL", False)
