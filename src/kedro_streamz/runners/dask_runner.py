from kedro.io import DataCatalog
from kedro.pipeline import Pipeline
from kedro.runner import AbstractRunner
from dask.distributed import Client, as_completed

class DaskRunner(AbstractRunner):
    """Kedro runner for executing pipelines using Dask."""

    def __init__(self, client: Client = None):
        """
        Initializes the DaskRunner.

        Args:
            client: Dask distributed client. If None, a new local client will be created.
        """
        self._client = client or Client()

    def _run(self, pipeline: Pipeline, catalog: DataCatalog) -> None:
        """Runs the pipeline using Dask.

        Args:
            pipeline: Kedro pipeline object.
            catalog: Data catalog for data persistence.
        """
        # Here, we convert each node in the Kedro pipeline to a Dask delayed object
        # and then compute them. This is a basic example; in reality, you might need
        # more complex integration depending on your needs.

        delayed_tasks = []

        for node in pipeline.nodes:
            task = self._client.submit(node.run, catalog, None)
            delayed_tasks.append(task)

        # Asynchronously gather results
        for future in as_completed(delayed_tasks):
            # You can process results or errors here
            if future.status == 'error':
                self._logger.error(f"Node {future.key} failed with: {future.exception()}")
            else:
                self._logger.info(f"Node {future.key} completed successfully")

    def create_default_data_set(self, ds_name: str):
        """This method is abstract in the base class and must be overridden."""
        # Implement method if you need custom behavior
        pass

    def run(self, pipeline: Pipeline, catalog: DataCatalog) -> None:
        """Public method to run the pipeline."""
        self._run(pipeline, catalog)
