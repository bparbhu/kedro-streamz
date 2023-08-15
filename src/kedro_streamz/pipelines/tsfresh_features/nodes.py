from kedro.io import DataCatalog
import pandas as pd
from tsfresh import extract_features
from tsfresh.utilities.dataframe_functions import roll_time_series
from dask import dataframe as dd

def extract_time_series_features(catalog: DataCatalog) -> pd.DataFrame:
    """
    Extract features from time series data using tsfresh.

    Args:
        catalog: Kedro DataCatalog to load the input data.

    Returns:
        A DataFrame with extracted features.
    """
    # Load the DataFrame from the catalog
    df = catalog.load("raw_time_series")

    # Optionally use Dask for distributed computation.
    # Convert pandas dataframe to Dask dataframe.
    dask_df = dd.from_pandas(df, npartitions=4)

    # Rolling the dataframe for window-based features.
    rolled_df = roll_time_series(dask_df.compute(), column_id="id", column_sort="time")

    # Using tsfresh to extract features.
    features = extract_features(rolled_df, column_id="id", column_sort="time")

    return features

# Further nodes or functions as needed.


# If you're using streamz, you might have additional functions here, like:

def stream_extract_features(data_stream):
    """
    Extract features from streaming time series data using tsfresh and Streamz.

    Args:
        data_stream: Streamz data stream of time series data.

    Returns:
        Streamz data stream with extracted features.
    """
    return data_stream.map(extract_time_series_features)

