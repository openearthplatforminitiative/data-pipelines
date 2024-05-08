import pandas as pd
import xarray as xr


def process_chunk(
    chunk_ds: xr.Dataset,
    cols_to_drop: list | None = None,
    drop_na_subset: list | None = None,
    drop_index: bool = False,
) -> pd.DataFrame:
    """
    Process a chunk of a raster in xarray.dataset format to a pandas dataframe.

    - Args:
    - chunk_ds (xarray.Dataset): The xarray dataset chunk.
    - cols_to_drop (list): List of columns to drop from the resulting dataframe.
    - drop_na_subset (list): List of columns to use for dropping rows with NA values.
    - drop_index (bool): Whether to drop the index column when resetting the index.

    - Returns:
    pd.DataFrame: The resulting pandas dataframe.
    """
    # Convert the chunk to a DataFrame
    df_chunk = chunk_ds.to_dataframe()

    # Drop unwanted columns
    if cols_to_drop is not None:
        for col in cols_to_drop:
            if col in df_chunk.columns:
                df_chunk = df_chunk.drop(columns=col)

    # Drop rows with NA values
    if drop_na_subset is not None:
        df_chunk = df_chunk.dropna(subset=drop_na_subset)

    df_chunk = df_chunk.reset_index(drop=drop_index)
    return df_chunk


def dataset_to_dataframe(
    ds: xr.Dataset,
    cols_to_drop: list | None = None,
    drop_na_subset: list | None = None,
    drop_index: bool = False,
    in_chunks: bool = False,
) -> pd.DataFrame:
    """
    Convert a raster in xarray.dataset format to a pandas dataframe.

    - Args:
    - ds (xarray.Dataset): The xarray dataset.
    - cols_to_drop (list): List of columns to drop from the resulting dataframe.
    - drop_na_subset (list): List of columns to use for dropping rows with NA values.
    - drop_index (bool): Whether to drop the index column when resetting the index.
    - in_chunks (bool): Whether to process the dataset in chunks.

    - Returns:
    pd.DataFrame: The resulting pandas dataframe.
    """
    if not in_chunks:
        return process_chunk(ds, cols_to_drop, drop_na_subset, drop_index)

    # List to collect DataFrames
    chunked_dataframes = []

    # Iterate over each chunk
    for i in range(0, len(ds["number"]), 1):
        chunk_ds = ds.isel(number=slice(i, i + 1))
        df_processed = process_chunk(chunk_ds, cols_to_drop, drop_na_subset, drop_index)

        # Append processed DataFrame to the list
        chunked_dataframes.append(df_processed)

    # Concatenate all DataFrames into a single DataFrame
    df = pd.concat(chunked_dataframes, ignore_index=True)

    return df
