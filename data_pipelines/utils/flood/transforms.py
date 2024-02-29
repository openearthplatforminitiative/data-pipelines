import dask.dataframe as dd
import numpy as np
import pandas as pd

from data_pipelines.utils.flood.config import (
    GLOFAS_FLOOD_INTENSITIES,
    GLOFAS_FLOOD_PEAK_TIMINGS,
    GLOFAS_FLOOD_TENDENCIES,
    GLOFAS_RET_PRD_THRESH_VALS,
)


def compute_flood_threshold_percentages(
    forecast_df: dd.DataFrame,
    threshold_df: dd.DataFrame,
    ret_period_vals: list[int] = GLOFAS_RET_PRD_THRESH_VALS,
) -> dd.DataFrame:
    """
    Compute the flood threshold percentages for the forecast DataFrame.

    - Args:
    - forecast_df (dd.DataFrame): The forecast DataFrame.
    - threshold_df (dd.DataFrame): The threshold DataFrame.
    - ret_period_vals (list[int]): The return period values.

    - Returns:
    dd.DataFrame: The forecast DataFrame with the threshold percentages.
    """
    # Merge forecast dataframe with threshold dataframe on latitude and longitude
    joined_ddf = dd.merge(
        forecast_df, threshold_df, on=["latitude", "longitude"], how="left"
    )

    threshold_cols = [f"threshold_{int(threshold)}y" for threshold in ret_period_vals]

    # Create columns for exceedance
    for threshold, col_name in zip(ret_period_vals, threshold_cols):
        exceed_col = f"exceed_{int(threshold)}y"
        joined_ddf[exceed_col] = (joined_ddf["dis24"] >= joined_ddf[col_name]).astype(
            "int64"
        )

    q1_fun = dd.Aggregation(
        name="q1", chunk=lambda s: s.quantile(0.25), agg=lambda s0: s0.quantile(0.25)
    )

    median_fun = dd.Aggregation(
        name="median", chunk=lambda s: s.median(), agg=lambda s0: s0.sum()
    )

    q3_fun = dd.Aggregation(
        name="q3", chunk=lambda s: s.quantile(0.75), agg=lambda s0: s0.quantile(0.75)
    )

    detailed_forecast_df = (
        joined_ddf.groupby(["latitude", "longitude", "issued_on", "valid_for", "step"])
        .agg(
            min_dis=("dis24", "min"),
            q1_dis=("dis24", q1_fun),
            median_dis=("dis24", median_fun),
            q3_dis=("dis24", q3_fun),
            max_dis=("dis24", "max"),
            p_above_2y=("exceed_2y", "mean"),
            p_above_5y=("exceed_5y", "mean"),
            p_above_20y=("exceed_20y", "mean"),
            numeric_only=pd.NamedAgg("dis24", "first"),
        )
        .reset_index()
        .drop(columns=["numeric_only"])
    )

    return detailed_forecast_df


def compute_flood_peak_timing(
    detailed_forecast_df: dd.DataFrame,
    col_name: str = "peak_timing",
    peak_timings: dict[str, str] = GLOFAS_FLOOD_PEAK_TIMINGS,
) -> dd.DataFrame:
    """
    Compute the flood peak timing for the forecast DataFrame.

    - Args:
    - detailed_forecast_df (dd.DataFrame): The detailed forecast DataFrame.
    - col_name (str): The name of the column to add.
    - peak_timings (dict[str, str]): The mapping from peak timings to border colors.

    - Returns:
    dd.DataFrame: The forecast DataFrame with the peak timings.
    """
    df_for_timing = detailed_forecast_df.drop(
        columns=["min_dis", "q1_dis", "q3_dis", "max_dis", "control_dis"]
    )

    # 1. Filter rows between steps 1 to 10
    filtered_ddf = df_for_timing[
        (df_for_timing["step"] >= 1) & (df_for_timing["step"] <= 10)
    ]

    # 2. Compute the maximum flood probability above the 2-year return period threshold for the first ten days
    max_ddf = (
        filtered_ddf.groupby(["latitude", "longitude"])
        .agg({"p_above_2y": "max"})
        .rename(columns={"p_above_2y": "max_2y_start"})
        .reset_index()
    )

    # 3. Join the max probabilities back to the main DataFrame
    df = dd.merge(df_for_timing, max_ddf, on=["latitude", "longitude"], how="left")

    def condition_func(df):
        df["condition"] = np.where(
            df["p_above_20y"] >= 0.3,
            4,
            np.where(
                df["p_above_5y"] >= 0.3, 3, np.where(df["p_above_2y"] >= 0.3, 2, 1)
            ),
        )
        return df

    ddf_conds = df.map_partitions(condition_func)

    ddf_conds = ddf_conds.drop(columns=["p_above_2y", "p_above_5y", "p_above_20y"])

    def sort_and_select_first_row(df):
        sorted_df = df.sort_values(
            ["latitude", "longitude", "condition", "median_dis"], ascending=False
        )
        first_row_df = (
            sorted_df.groupby(["latitude", "longitude"]).first().reset_index()
        )
        return first_row_df

    ddf_maps = ddf_conds.map_partitions(sort_and_select_first_row, meta=ddf_conds).drop(
        columns=["median_dis"]
    )

    # Rename step column to peak_step
    ddf_maps = ddf_maps.rename(columns={"step": "peak_step", "valid_for": "peak_day"})

    def peak_timing_func(df):
        df[col_name] = np.where(
            (df["peak_step"].isin(range(1, 4))) & (df["max_2y_start"] >= 0.30),
            peak_timings["black_border"],
            np.where(
                (df["peak_step"] > 10) & (df["max_2y_start"] < 0.30),
                peak_timings["grayed_color"],
                peak_timings["gray_border"],
            ),
        )
        return df

    ddf = ddf_maps.map_partitions(peak_timing_func).drop(
        columns=["condition", "max_2y_start"]
    )

    return ddf


def compute_flood_tendency(
    detailed_forecast_df: dd.DataFrame,
    col_name: str = "tendency",
    tendencies: dict[str, str] = GLOFAS_FLOOD_TENDENCIES,
) -> dd.DataFrame:
    """
    Compute the flood tendency for the forecast DataFrame.

    - Args:
    - detailed_forecast_df (dd.DataFrame): The detailed forecast DataFrame.
    - col_name (str): The name of the column to add.
    - tendencies (dict[str, str]): The mapping from tendencies to shapes.

    - Returns:
    dd.DataFrame: The forecast DataFrame with the tendencies.
    """
    df_for_tendency = detailed_forecast_df.drop(
        columns=["q1_dis", "q3_dis", "p_above_2y", "p_above_5y", "p_above_20y"]
    )

    grid_cell_tendency = (
        df_for_tendency.groupby(["latitude", "longitude"])
        .agg(
            max_median_dis=("median_dis", "max"),
            min_median_dis=("median_dis", "min"),
            control_dis=("control_dis", "first"),
            max_max_dis=("max_dis", "max"),
            min_min_dis=("min_dis", "min"),
            numeric_only=pd.NamedAgg("max_dis", "first"),
        )
        .reset_index()
        .drop(columns=["numeric_only"])
    )

    def tendency_func(df):
        df[col_name] = np.where(
            df["max_median_dis"] > df["control_dis"] * 1.10,
            tendencies["increasing"],
            np.where(
                (df["min_median_dis"] <= df["control_dis"] * 0.90)
                & (df["max_median_dis"] <= df["control_dis"] * 1.10),
                tendencies["decreasing"],
                tendencies["stagnant"],
            ),
        )
        return df

    grid_cell_tendency = grid_cell_tendency.map_partitions(tendency_func)

    return grid_cell_tendency


def compute_flood_intensity(
    detailed_forecast_df: dd.DataFrame,
    col_name: str = "intensity",
    intensities: dict[str, str] = GLOFAS_FLOOD_INTENSITIES,
) -> dd.DataFrame:
    """
    Compute the flood intensity for the forecast DataFrame.

    - Args:
    - detailed_forecast_df (dd.DataFrame): The detailed forecast DataFrame.
    - col_name (str): The name of the column to add.
    - intensities (dict[str, str]): The mapping from flood intensities to colors.

    - Returns:
    dd.DataFrame: The forecast DataFrame with the intensities.
    """
    df_for_intensity = detailed_forecast_df.drop(
        columns=["min_dis", "q1_dis", "q3_dis", "max_dis", "control_dis"]
    )

    grid_cell_intensity = (
        df_for_intensity.groupby(["latitude", "longitude"])
        .agg(
            max_p_above_20y=("p_above_20y", "max"),
            max_p_above_5y=("p_above_5y", "max"),
            max_p_above_2y=("p_above_2y", "max"),
            numeric_only=pd.NamedAgg("p_above_20y", "first"),
        )
        .reset_index()
        .drop(columns=["numeric_only"])
    )

    def intensity_func(df):
        df[col_name] = np.where(
            df["max_p_above_20y"] >= 0.30,
            intensities["purple"],
            np.where(
                df["max_p_above_5y"] >= 0.30,
                intensities["red"],
                np.where(
                    df["max_p_above_2y"] >= 0.30,
                    intensities["yellow"],
                    intensities["gray"],
                ),
            ),
        )
        return df

    grid_cell_intensity = grid_cell_intensity.map_partitions(intensity_func)

    return grid_cell_intensity


def add_geometry(
    df: pd.DataFrame | dd.core.DataFrame, half_grid_size: float, precision: int
) -> pd.DataFrame | dd.core.DataFrame:
    """
    Add a geometry column to the DataFrame.

    - Args:
    - df (pd.DataFrame | dd.core.DataFrame): The DataFrame.
    - half_grid_size (float): The half grid size.
    - precision (int): The precision.

    - Returns:
    pd.DataFrame | dd.core.DataFrame: The DataFrame with the geometry column.
    """
    df["min_latitude"] = (df["latitude"] - half_grid_size).round(precision)
    df["max_latitude"] = (df["latitude"] + half_grid_size).round(precision)
    df["min_longitude"] = (df["longitude"] - half_grid_size).round(precision)
    df["max_longitude"] = (df["longitude"] + half_grid_size).round(precision)

    df["wkt"] = (
        "POLYGON (("
        + df["min_longitude"].astype(str)
        + " "
        + df["min_latitude"].astype(str)
        + ","
        + df["min_longitude"].astype(str)
        + " "
        + df["max_latitude"].astype(str)
        + ","
        + df["max_longitude"].astype(str)
        + " "
        + df["max_latitude"].astype(str)
        + ","
        + df["max_longitude"].astype(str)
        + " "
        + df["min_latitude"].astype(str)
        + ","
        + df["min_longitude"].astype(str)
        + " "
        + df["min_latitude"].astype(str)
        + "))"
    )

    df = df.drop(
        ["min_latitude", "max_latitude", "min_longitude", "max_longitude"], axis=1
    )

    return df
