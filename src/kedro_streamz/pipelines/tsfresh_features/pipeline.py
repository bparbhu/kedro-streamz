from kedro.pipeline import Pipeline, node

from .nodes import extract_time_series_features

def create_pipeline(**kwargs):
    """Create the tsfresh feature extraction pipeline."""
    return Pipeline(
        [
            node(
                func=extract_time_series_features,
                inputs="raw_time_series",
                outputs="time_series_features",
                name="node_extract_time_series_features",
            ),
            # Add more nodes as needed
        ]
    )
