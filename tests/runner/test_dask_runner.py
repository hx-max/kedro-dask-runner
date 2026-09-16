from __future__ import annotations

import pytest

from kedro.framework.hooks import _create_hook_manager
from kedro.io import DataCatalog, MemoryDataset
from kedro.pipeline import node, pipeline
from kedro.runner import DaskRunner
from tests.conftest import PersistentTestDataset
from tests.runner.conftest import exception_fn, identity


@pytest.fixture(scope="module")
def shared_client():
    """A local threaded Dask cluster shared by the tests of this module.

    Starting a cluster is not free, so the cluster is created once and injected into
    the runner. The tests that care about the cluster lifecycle start their own.
    """
    from distributed import Client, LocalCluster

    cluster = LocalCluster(
        n_workers=2, threads_per_worker=1, processes=False, dashboard_address=None
    )
    client = Client(cluster)
    yield client
    client.close()
    cluster.close()


class _FakeCluster:
    """Stands in for a ``LocalCluster`` when only its kind matters."""

    def __init__(self, processes: bool):
        self.processes = processes


class _FakeClient:
    """Stands in for a ``distributed.Client`` when only its cluster kind matters.

    The validation methods only look at whether the workers run in the same process
    as the runner, so a two attribute fake tests them faster and more deterministically
    than a real cluster of processes would.
    """

    def __init__(self, processes: bool):
        self.cluster = _FakeCluster(processes)


def _runner_with_cluster_kind(processes: bool) -> DaskRunner:
    """A runner whose active client describes a cluster of the given kind."""
    runner = DaskRunner(processes=processes)
    runner._active_client = _FakeClient(processes)
    return runner


class TestValidDaskRunner:
    def test_dask_run(self, shared_client, fan_out_fan_in, catalog):
        catalog["A"] = 42
        result = DaskRunner(client=shared_client).run(fan_out_fan_in, catalog)
        assert "Z" in result
        assert result["Z"].load() == (42, 42, 42)

    def test_dask_run_with_plugin_manager(self, shared_client, fan_out_fan_in, catalog):
        catalog["A"] = 42
        result = DaskRunner(client=shared_client).run(
            fan_out_fan_in, catalog, hook_manager=_create_hook_manager()
        )
        assert result["Z"].load() == (42, 42, 42)

    def test_memory_dataset_input(self, shared_client, fan_out_fan_in):
        catalog = DataCatalog({"A": MemoryDataset("42")})
        result = DaskRunner(client=shared_client).run(fan_out_fan_in, catalog)
        assert "Z" in result
        assert result["Z"].load() == ("42", "42", "42")

    def test_empty_pipeline(self, shared_client):
        result = DaskRunner(client=shared_client).run(pipeline([]), DataCatalog())
        assert result == {}

    def test_injected_client_is_left_running(self, fan_out_fan_in, catalog):
        from distributed import Client, LocalCluster

        cluster = LocalCluster(
            n_workers=2, threads_per_worker=1, processes=False, dashboard_address=None
        )
        client = Client(cluster)
        try:
            catalog["A"] = 42
            DaskRunner(client=client).run(fan_out_fan_in, catalog)
            # the runner does not own the client, so it must not close it: the same
            # client has to be usable for a second run
            assert client.status == "running"
            result = DaskRunner(client=client).run(fan_out_fan_in, catalog)
            assert result["Z"].load() == (42, 42, 42)
        finally:
            client.close()
            cluster.close()

    def test_node_failure_is_propagated(self, shared_client):
        catalog = DataCatalog({"A": MemoryDataset(42)})
        test_pipeline = pipeline([node(exception_fn, "A", "B")])

        with pytest.raises(Exception, match="test exception"):
            DaskRunner(client=shared_client).run(test_pipeline, catalog)


class TestClusterLifecycle:
    def test_own_cluster_is_created_from_the_constructor_and_closed(
        self, mocker, fan_out_fan_in, catalog
    ):
        from distributed import LocalCluster

        created = []

        def spy(*args, **kwargs):
            cluster = LocalCluster(*args, **kwargs)
            created.append((cluster, kwargs))
            return cluster

        mocker.patch("distributed.LocalCluster", side_effect=spy)

        catalog["A"] = 42
        result = DaskRunner(
            n_workers=3, threads_per_worker=2, dashboard_address=None
        ).run(fan_out_fan_in, catalog)

        assert result["Z"].load() == (42, 42, 42)
        cluster, kwargs = created[0]
        assert kwargs["n_workers"] == 3
        assert kwargs["threads_per_worker"] == 2
        assert kwargs["processes"] is False
        # the runner owns the cluster, so it closes it when the run is over
        assert cluster.status.name == "closed"

    def test_client_kwargs_are_forwarded_to_the_cluster(self, mocker, fan_out_fan_in, catalog):
        from distributed import LocalCluster

        created = []

        def spy(*args, **kwargs):
            cluster = LocalCluster(*args, **kwargs)
            created.append(kwargs)
            return cluster

        mocker.patch("distributed.LocalCluster", side_effect=spy)

        catalog["A"] = 42
        DaskRunner(memory_limit="512MB", dashboard_address=None).run(
            fan_out_fan_in, catalog
        )

        assert created[0]["memory_limit"] == "512MB"


class TestInitValidation:
    @pytest.mark.parametrize("n_workers", [0, -1, 1.5, "2", True])
    def test_error_when_n_workers_is_not_a_positive_integer(self, n_workers):
        with pytest.raises(ValueError, match="n_workers should be a positive integer"):
            DaskRunner(n_workers=n_workers)

    @pytest.mark.parametrize("threads_per_worker", [0, -1, 2.5, "2"])
    def test_error_when_threads_per_worker_is_not_a_positive_integer(
        self, threads_per_worker
    ):
        with pytest.raises(
            ValueError, match="threads_per_worker should be a positive integer"
        ):
            DaskRunner(threads_per_worker=threads_per_worker)

    @pytest.mark.parametrize("processes", ["yes", 1, None])
    def test_error_when_processes_is_not_boolean(self, processes):
        with pytest.raises(ValueError, match="processes takes only booleans"):
            DaskRunner(processes=processes)

    @pytest.mark.parametrize("retries", [-1, 1.5, "2", True])
    def test_error_when_retries_is_not_a_non_negative_integer(self, retries):
        with pytest.raises(ValueError, match="retries should be a non-negative integer"):
            DaskRunner(retries=retries)

    def test_params_are_exposed(self):
        runner = DaskRunner(
            n_workers=4, threads_per_worker=2, processes=True, retries=1
        )
        assert runner.n_workers == 4
        assert runner.threads_per_worker == 2
        assert runner.processes is True
        assert runner.retries == 1


class TestScheduling:
    def test_dependency_order_is_respected(self, shared_client):
        """Each node must see the output of the node it depends on."""
        catalog = DataCatalog({"A": MemoryDataset(1)})
        test_pipeline = pipeline(
            [
                node(lambda x: x + 1, "A", "B"),
                node(lambda x: x * 10, "B", "C"),
                node(lambda x: x - 5, "C", "D"),
            ]
        )

        result = DaskRunner(client=shared_client).run(test_pipeline, catalog)
        assert result["D"].load() == 15

    def test_required_workers_count_is_capped_by_n_workers(self, fan_out_fan_in):
        assert DaskRunner(n_workers=2)._get_required_workers_count(fan_out_fan_in) == 2
        # the pipeline has 3 independent nodes, so 3 workers are enough
        assert DaskRunner(n_workers=99)._get_required_workers_count(fan_out_fan_in) == 3

    def test_get_executor_returns_none(self, fan_out_fan_in):
        """DaskRunner schedules through Dask, not through a concurrent.futures pool."""
        assert DaskRunner()._get_executor(3) is None


class TestDistributedValidation:
    def test_memory_datasets_are_rejected_when_workers_are_processes(
        self, fan_out_fan_in, catalog
    ):
        catalog["A"] = 42
        runner = _runner_with_cluster_kind(processes=True)

        with pytest.raises(ValueError, match="only exist in memory"):
            runner._validate_shared_memory_datasets(fan_out_fan_in, catalog)

    def test_memory_datasets_are_accepted_when_workers_are_threads(
        self, fan_out_fan_in, catalog
    ):
        catalog["A"] = 42
        runner = _runner_with_cluster_kind(processes=False)

        # no exception: threads share the runner's memory
        runner._validate_shared_memory_datasets(fan_out_fan_in, catalog)

    def test_persistent_datasets_pass_validation(self, persistent_dataset_catalog):
        test_pipeline = pipeline(
            [
                node(identity, "ds0_A", "ds2_A"),
                node(identity, "ds2_A", "dsX"),
                node(identity, "ds0_B", "ds2_B"),
                node(identity, "ds2_B", "dsY"),
            ]
        )
        runner = _runner_with_cluster_kind(processes=True)

        runner._validate_shared_memory_datasets(test_pipeline, persistent_dataset_catalog)

    def test_parameters_are_never_flagged(self, persistent_dataset_catalog):
        """``params:p`` is a MemoryDataset, but it is resolved from the workers' own
        configuration, so it never needs to be shared."""
        test_pipeline = pipeline([node(identity, "params:p", "dsX", name="node1")])
        runner = _runner_with_cluster_kind(processes=True)

        runner._validate_shared_memory_datasets(test_pipeline, persistent_dataset_catalog)

    def test_single_process_datasets_are_rejected(self):
        class SingleProcessDataset(PersistentTestDataset):
            _SINGLE_PROCESS = True

        catalog = DataCatalog({"ds0_A": SingleProcessDataset()})
        runner = _runner_with_cluster_kind(processes=True)

        with pytest.raises(AttributeError, match="cannot be serialised"):
            runner._validate_catalog(catalog)

    def test_persistent_catalog_passes_validation(self, persistent_dataset_catalog):
        runner = _runner_with_cluster_kind(processes=True)
        runner._validate_catalog(persistent_dataset_catalog)

    def test_unserialisable_nodes_are_rejected(self):
        """A node capturing something the workers cannot rebuild cannot be shipped.

        A database or file handle held by a node is the realistic version of this; the
        class below is the version that fails identically on every machine.
        """

        class UnpicklableResource:
            def __reduce__(self):
                raise TypeError("this resource cannot leave this process")

        resource = UnpicklableResource()

        def reads_from_a_resource(arg):
            return resource

        runner = _runner_with_cluster_kind(processes=True)
        with pytest.raises(AttributeError, match="cannot be serialised"):
            runner._validate_nodes(pipeline([node(reads_from_a_resource, "A", "B")]).nodes)

    def test_lambda_nodes_are_accepted_when_workers_are_threads(self, shared_client):
        """A threaded cluster never ships anything, so closures stay usable."""
        catalog = DataCatalog({"A": MemoryDataset(21)})
        test_pipeline = pipeline([node(lambda x: x * 2, "A", "B")])

        assert DaskRunner(client=shared_client).run(test_pipeline, catalog)["B"].load() == 42
