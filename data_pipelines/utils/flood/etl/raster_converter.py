import logging
import dagster

import pandas as pd
import xarray as xr

logging.basicConfig(level=logging.INFO)


class RasterConverter:
    """
    Class for converting raster (NetCDF and GRIB) files
    to pandas dataframes and parquet files.
    """

    def file_to_parquet(
        self,
        input_path: str,
        output_path: str,
        read_engine: str | None = None,
        write_engine: str = "pyarrow",
        compression: str = "snappy",
        cols_to_drop: list | None = None,
        cols_to_rename: dict | None = None,
        drop_na_subset: list | None = None,
        drop_index: bool = False,
        save_index: bool = None,
        context: dagster.AssetExecutionContext = None,
    ) -> None:
        """
        Convert a raster (GRIB or NetCDF) file to Parquet format.

        - Args:
        - input_path (str): Path to the raster file.
        - output_path (str): Path to save the resulting Parquet file.
        - read_engine (str): The engine to use for reading the raster file.
        - write_engine (str): The engine to use for writing the Parquet file.
        - compression (str): The compression algorithm to use for writing the Parquet file.
        - cols_to_drop (list): List of columns to drop from the resulting dataframe.
        - cols_to_rename (dict): Dictionary where keys are original column names and values are the new names.
        - drop_na_subset (list): List of columns to use for dropping rows with NA values.
        - drop_index (bool): Whether to drop the index column when resetting the index.
        - save_index (bool): Whether to save the index column.
        - context (dagster.AssetExecutionContext): The context object for the asset.

        - Returns:
        None
        """
        try:
            # Read the raster file into an xarray Dataset
            ds = xr.open_dataset(input_path, engine=read_engine)

            # Convert the xarray Dataset to a Pandas DataFrame
            df = ds.to_dataframe().reset_index(drop=drop_index)

            # Drop unwanted columns
            if cols_to_drop is not None:
                for col in cols_to_drop:
                    if col in df.columns:
                        df = df.drop(columns=col)

            # Rename columns
            if cols_to_rename is not None:
                df = df.rename(columns=cols_to_rename)

            # Drop rows with NA values
            if drop_na_subset is not None:
                df = df.dropna(subset=drop_na_subset)

            # Step 3: Convert the Pandas DataFrame to a Parquet file using pyarrow and snappy compression
            df.to_parquet(
                output_path,
                engine=write_engine,
                compression=compression,
                index=save_index,
            )
            if context is not None:
                context.log.info(
                    f"Converted {input_path} to {output_path} successfully!"
                )
            else:
                logging.info(f"Converted {input_path} to {output_path} successfully!")

        except Exception as e:
            if context is not None:
                context.log.error(
                    f"Error during conversion of {input_path} to {output_path}: {e}"
                )
            else:
                logging.error(
                    f"Error during conversion of {input_path} to {output_path}: {e}"
                )

    def dataset_to_dataframe(
        self,
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
        try:
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

            logging.info(f"Converted xarray dataset to pandas dataframe successfully!")

            return df

        except Exception as e:
            logging.error(
                f"Error during conversion of xarray dataset to pandas dataframe: {e}"
            )
            return None

    def dataframe_to_parquet(
        self,
        df: pd.DataFrame,
        output_path: str,
        write_engine: str = "pyarrow",
        compression: str = "snappy",
        save_index: bool | None = None,
    ) -> None:
        """
        Save a pandas dataframe in Parquet format.

        - Args:
        - df (pd.DataFrame): The pandas dataframe.
        - output_path (str): Path to save the resulting Parquet file.
        - write_engine (str): The engine to use for writing the Parquet file.
        - compression (str): The compression algorithm to use for writing the Parquet file.
        - save_index (bool): Whether to save the index column.

        - Returns:
        None
        """
        try:
            df.to_parquet(
                output_path,
                engine=write_engine,
                compression=compression,
                index=save_index,
            )

            logging.info(f"Converted pandas dataframe to {output_path} successfully!")

        except Exception as e:
            logging.error(
                f"Error during conversion of pandas dataframe to {output_path}: {e}"
            )
