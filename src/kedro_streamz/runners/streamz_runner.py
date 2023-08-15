from kedro.io import DataCatalog
from kedro.pipeline import Pipeline
from kedro.runner import AbstractRunner
from streamz import Stream

class StreamzRunner(AbstractRunner):
    """Kedro runner for executing pipelines using Streamz."""

    def __init__(self):
        # Initialize the main Stream instance
        self._source = Stream()

    def _run(self, pipeline: Pipeline, catalog: DataCatalog) -> None:
        """Runs the pipeline using Streamz.

        Args:
            pipeline: Kedro pipeline object.
            catalog: Data catalog for data persistence.
        """
        # Here, we take the source stream and pass it through each node in the pipeline
        current_stream = self._source

        for node in pipeline.nodes:
            current_stream = current_stream.map(node.run, catalog, None)

        # Optionally, you can have a sink to collect results or to handle errors
        current_stream.sink(self._handle_output)

    def _handle_output(self, result):
        """Handle the results from the pipeline.

        Args:
            result: Output from the last node in the pipeline.
        """
        # Do something with the result, e.g., logging, storing, or sending to an external system.
        pass

    def emit(self, data):
        """Emit data into the pipeline.

        Args:
            data: Data to be emitted into the Streamz pipeline.
        """
        self._source.emit(data)

    def create_default_data_set(self, ds_name: str):
        """This method is abstract in the base class and must be overridden."""
        # Implement method if you need custom behavior
        pass

    def run(self, pipeline: Pipeline, catalog: DataCatalog) -> None:
        """Public method to run the pipeline."""
        self._run(pipeline, catalog)
