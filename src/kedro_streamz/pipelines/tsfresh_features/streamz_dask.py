import pandas as pd
from tsfresh import extract_features
from tsfresh.utilities.dataframe_functions import roll_time_series
from streamz import Stream
from dask import dataframe as dd
from kedro.io import DataCatalog

def daskify_dataframe(df: pd.DataFrame, npartitions=4) -> dd.DataFrame:
    """
    Convert a pandas DataFrame to a Dask DataFrame.

    Args:
        df: Pandas DataFrame.
        npartitions: Number of partitions for the Dask DataFrame.

    Returns:
        A Dask DataFrame.
    """
    return dd.from_pandas(df, npartitions=npartitions)

def extract_time_series_features_with_dask(df: dd.DataFrame) -> dd.DataFrame:
    """
    Extract features from time series data using tsfresh and Dask.

    Args:
        df: Raw time series data in a Dask DataFrame.

    Returns:
        A Dask DataFrame with extracted features.
    """
    # Rolling the dataframe for window-based features.
    rolled_df = roll_time_series(df.compute(), column_id="id", column_sort="time")

    # Using tsfresh to extract features.
    features = extract_features(rolled_df, column_id="id", column_sort="time")

    return daskify_dataframe(features)

def process_stream_with_dask(stream: Stream, catalog: DataCatalog) -> Stream:
    """
    Process a Streamz data stream using tsfresh and Dask.

    Args:
        stream: Streamz data stream of time series data.
        catalog: Kedro DataCatalog for loading data.

    Returns:
        Streamz data stream with extracted features.
    """
    return stream.map(catalog.load).map(daskify_dataframe).map(extract_time_series_features_with_dask)

