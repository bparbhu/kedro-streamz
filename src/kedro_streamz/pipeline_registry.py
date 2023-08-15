"""Project pipelines."""
from __future__ import annotations

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline
from pipelines.tsfresh_features.pipeline import create_pipeline as tsfresh_pipeline

def register_pipelines() -> dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    pipelines = find_pipelines()

    # Register tsfresh_features pipeline
    pipelines["tsfresh_features"] = tsfresh_pipeline()

    # If you want the default pipeline to run all discovered pipelines at once:
    pipelines["__default__"] = sum(pipelines.values())

    return pipelines
