# src/my_project/pipelines/tsfresh_features/pipeline.py

from kedro.pipeline import Pipeline, node
from .nodes import (
    extract_time_series_features,
    store_features_redshift,
    store_features_bigquery
)

def create_pipeline(**kwargs):
    return Pipeline(
        [
            # Extracting tsfresh features
            node(
                extract_time_series_features,
                "raw_time_series",
                "features",
                name="tsfresh_calculation"
            ),

            # Storing features in AWS Redshift
            node(
                store_features_redshift,
                "features",
                None,
                name="store_redshift"
            ),

            # Storing features in BigQuery
            node(
                store_features_bigquery,
                "features",
                None,
                name="store_bigquery"
            )
        ]
    )
