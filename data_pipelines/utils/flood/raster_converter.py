import pandas as pd
import xarray as xr


def dataset_to_dataframe(
    ds: xr.Dataset,
    cols_to_drop: list | None = None,
    drop_na_subset: list | None = None,
    drop_index: bool = False,
) -> pd.DataFrame:
    """
    Convert a raster in xarray.dataset format to a pandas dataframe.

    - Args:
    - ds (xarray.Dataset): The xarray dataset.
    - cols_to_drop (list): List of columns to drop from the resulting dataframe.
    - drop_na_subset (list): List of columns to use for dropping rows with NA values.
    - drop_index (bool): Whether to drop the index column when resetting the index.

    - Returns:
    pd.DataFrame: The resulting pandas dataframe.
    """
    df = ds.to_dataframe()

    # Drop unwanted columns
    if cols_to_drop is not None:
        for col in cols_to_drop:
            if col in df.columns:
                df = df.drop(columns=col)

    # Drop rows with NA values
    if drop_na_subset is not None:
        df = df.dropna(subset=drop_na_subset)

    df = df.reset_index(drop=drop_index)

    return df
