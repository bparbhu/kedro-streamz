from kedro.io import DataCatalog
import pandas as pd
from tsfresh import extract_features
from tsfresh.utilities.dataframe_functions import roll_time_series
from dask import dataframe as dd
from streamz import Stream
from sqlalchemy import create_engine

def extract_time_series_features(catalog: DataCatalog) -> pd.DataFrame:
    """
    Extract features from time series data using tsfresh.

    Args:
        catalog: Kedro DataCatalog to load the input data.

    Returns:
        A DataFrame with extracted features.
    """
    df = catalog.load("raw_time_series")  # Load the DataFrame from the catalog

    # Convert pandas dataframe to Dask dataframe.
    dask_df = dd.from_pandas(df, npartitions=4)

    # Rolling the dataframe for window-based features.
    rolled_df = roll_time_series(dask_df.compute(), column_id="id", column_sort="time")

    # Using tsfresh to extract features.
    features = extract_features(rolled_df, column_id="id", column_sort="time")

    return features

def stream_extract_features(catalog: DataCatalog) -> Stream:
    """
    Extract features from streaming time series data using tsfresh and Streamz.

    Args:
        catalog: Kedro DataCatalog to load the input data.

    Returns:
        Streamz data stream with extracted features.
    """
    source = Stream(scatter=True, asynchronous=True)
    data_stream = source.map(catalog.load("raw_time_series"))
    processed_stream = data_stream.map(extract_time_series_features)

    return processed_stream  # You can also return the source if needed to feed in data from external sources

def store_features_bigquery(features: pd.DataFrame) -> None:
    """
    Store the features DataFrame into a BigQuery table.

    Args:
        features: DataFrame containing tsfresh features.

    Returns:
        None
    """
    # Assuming you've set up GOOGLE_APPLICATION_CREDENTIALS
    features.to_gbq('your_project_id.your_dataset.your_table_name')

def store_features_redshift(features: pd.DataFrame) -> None:
    """
    Store the features DataFrame into a Redshift table.

    Args:
        features: DataFrame containing tsfresh features.

    Returns:
        None
    """
    engine = create_engine('postgresql+psycopg2://username:password@redshift-hostname:port/database')
    features.to_sql('your_table_name', engine, if_exists='replace', index=False)
