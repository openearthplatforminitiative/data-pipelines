from pyspark.sql import functions as F
from pyspark.sql.window import Window


def compute_flood_tendency(df, flood_tendencies, col_name="tendency"):
    # Compute flood tendency once per grid cell
    grid_cell_tendency = df.groupBy("latitude", "longitude").agg(
        F.max("median_dis").alias("max_median_dis"),
        F.min("median_dis").alias("min_median_dis"),
        F.first("control_dis").alias("control_dis"),
        F.max("max_dis").alias("max_max_dis"),
        F.min("min_dis").alias("min_min_dis"),
    )

    # Define the tendency based on aggregated values
    tendency_condition = (
        F.when(
            F.col("max_median_dis") > F.col("control_dis") * 1.10,
            flood_tendencies["increasing"],
        )
        .when(
            (F.col("min_median_dis") <= F.col("control_dis") * 0.90)
            & (F.col("max_median_dis") <= F.col("control_dis") * 1.10),
            flood_tendencies["decreasing"],
        )
        .otherwise(flood_tendencies["stagnant"])
    )

    return grid_cell_tendency.withColumn(col_name, tendency_condition)


def compute_flood_intensity(df, flood_intensities, col_name="intensity"):
    # Compute flood intensity once per grid cell
    grid_cell_intensity = df.groupBy("latitude", "longitude").agg(
        F.max("p_above_20y").alias("max_p_above_20y"),
        F.max("p_above_5y").alias("max_p_above_5y"),
        F.max("p_above_2y").alias("max_p_above_2y"),
    )

    # Define the color (flood intensity) based on aggregated values
    color_condition = (
        F.when(
            grid_cell_intensity["max_p_above_20y"] >= 0.30, flood_intensities["purple"]
        )
        .when((grid_cell_intensity["max_p_above_5y"] >= 0.30), flood_intensities["red"])
        .when(
            (grid_cell_intensity["max_p_above_2y"] >= 0.30), flood_intensities["yellow"]
        )
        .otherwise(flood_intensities["gray"])
    )

    return grid_cell_intensity.withColumn(col_name, color_condition)


def compute_flood_peak_timing(df, flood_peak_timings, col_name="peak_timing"):
    # 1. Filter rows between steps 1 to 10
    filtered_df = df.filter((F.col("step").between(1, 10)))

    # 2. Compute the maximum flood probability above the 2 year
    # return period threshold for the first ten days
    max_df = filtered_df.groupBy("latitude", "longitude").agg(
        F.max("p_above_2y").alias("max_2y_start")
    )

    # 3. Join the max probabilities back to the main DataFrame
    # Note: max_df used to be broadcasted, but this isn't strictly necessary
    # as the input DataFrame 'df' is already partitioned by latitude and longitude
    df = df.join(max_df, ["latitude", "longitude"], how="left")

    # Determine the conditions for each scenario
    df = df.withColumn(
        "condition",
        F.when(F.col("p_above_20y") >= 0.3, F.lit(1))
        .when(F.col("p_above_5y") >= 0.3, F.lit(2))
        .when(F.col("p_above_2y") >= 0.3, F.lit(3))
        .otherwise(F.lit(4)),
    )

    # 4. Compute the step_of_highest_severity
    windowSpec = Window.partitionBy("latitude", "longitude").orderBy(
        [F.asc("condition"), F.desc("median_dis")]
    )

    # Retrieve the first row (peak step) for each partition (lat, lon group)
    # Define the peak_day as the valid_for field of the peak forecast step
    df = (
        df.withColumn("row_num", F.row_number().over(windowSpec))
        .filter(F.col("row_num") == 1)
        .select(
            "latitude", "longitude", "max_2y_start", "issued_on", "step", "valid_for"
        )
        .withColumnRenamed("step", "peak_step")
        .withColumnRenamed("valid_for", "peak_day")
    )

    # Old method, using first() with sorted window may supposedly not lead to desired result
    # See: https://stackoverflow.com/a/33878701
    # windowSpec = Window.partitionBy("latitude", "longitude")
    # df = df.withColumn("peak_step", F.first("step").over(windowSpec.orderBy([F.asc("condition"), F.desc("median_dis")])))

    # 5. Determine the peak_timing column
    peak_condition = (
        F.when(F.col("peak_step").between(1, 3), flood_peak_timings["black_border"])
        .when(
            (F.col("peak_step") > 10) & (F.col("max_2y_start") < 0.30),
            flood_peak_timings["grayed_color"],
        )
        .otherwise(flood_peak_timings["gray_border"])
    )

    df = df.withColumn(col_name, peak_condition).drop("max_2y_start")

    return df


# Define a function to compute the percentage exceeding thresholds for the forecasts dataframe
def compute_flood_threshold_percentages(
    forecast_df, threshold_df, threshold_vals, accuracy_mode="approx"
):
    assert accuracy_mode in [
        "approx",
        "exact",
    ], "Accuracy mode must be either 'approx' or 'exact'."

    threshold_cols = [f"threshold_{int(threshold)}y" for threshold in threshold_vals]

    # Join forecast dataframe with threshold dataframe on latitude and longitude
    joined_df = forecast_df.join(threshold_df, on=["latitude", "longitude"], how="left")

    for threshold, col_name in zip(threshold_vals, threshold_cols):
        exceed_col = f"exceed_{int(threshold)}y"
        joined_df = joined_df.withColumn(
            exceed_col,
            F.when(joined_df["dis24"] >= joined_df[col_name], 1).otherwise(0),
        )

    # Aggregate to compute percentages
    agg_exprs = [
        F.mean(f"exceed_{int(threshold)}y").alias(f"p_above_{int(threshold)}y")
        for threshold in threshold_vals
    ]

    # Precompute values
    q1_dis = (
        F.percentile_approx("dis24", 0.25)
        if accuracy_mode == "approx"
        else F.expr("percentile(dis24, 0.25)")
    )
    median_dis = (
        F.percentile_approx("dis24", 0.5)
        if accuracy_mode == "approx"
        else F.expr("percentile(dis24, 0.5)")
    )
    q3_dis = (
        F.percentile_approx("dis24", 0.75)
        if accuracy_mode == "approx"
        else F.expr("percentile(dis24, 0.75)")
    )

    # Add 5-number summary computations for 'dis24' column
    agg_exprs.extend(
        [
            F.min("dis24").alias("min_dis"),
            q1_dis.alias("Q1_dis"),
            median_dis.alias("median_dis"),
            q3_dis.alias("Q3_dis"),
            F.max("dis24").alias("max_dis"),
        ]
    )

    results = joined_df.groupBy(
        "latitude", "longitude", "issued_on", "valid_for", "step"
    ).agg(*agg_exprs)

    return results


def add_geometry(df, half_grid_size, precision):
    """
    Add a geometry column to the DataFrame.

    :param df: The DataFrame.
    :param half_grid_size: The half grid size.
    :param precision: The precision to use for rounding.

    :return: The DataFrame with a geometry column.
    """
    return (
        df.withColumn(
            "min_latitude", F.round(F.col("latitude") - half_grid_size, precision)
        )
        .withColumn(
            "max_latitude", F.round(F.col("latitude") + half_grid_size, precision)
        )
        .withColumn(
            "min_longitude", F.round(F.col("longitude") - half_grid_size, precision)
        )
        .withColumn(
            "max_longitude", F.round(F.col("longitude") + half_grid_size, precision)
        )
        .withColumn(
            "wkt",
            F.concat(
                F.lit("POLYGON (("),
                F.col("min_longitude"),
                F.lit(" "),
                F.col("min_latitude"),
                F.lit(","),
                F.col("min_longitude"),
                F.lit(" "),
                F.col("max_latitude"),
                F.lit(","),
                F.col("max_longitude"),
                F.lit(" "),
                F.col("max_latitude"),
                F.lit(","),
                F.col("max_longitude"),
                F.lit(" "),
                F.col("min_latitude"),
                F.lit(","),
                F.col("min_longitude"),
                F.lit(" "),
                F.col("min_latitude"),
                F.lit("))"),
            ),
        )
        .drop("min_latitude")
        .drop("max_latitude")
        .drop("min_longitude")
        .drop("max_longitude")
    )
