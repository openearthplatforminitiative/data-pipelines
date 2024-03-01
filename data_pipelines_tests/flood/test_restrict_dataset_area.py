import unittest

from data_pipelines.utils.flood.utils import restrict_dataset_area
from data_pipelines_tests.flood.data_generation import (
    generate_restrict_dataset_area_test_dataset,
)


class TestRestrictArea(unittest.TestCase):
    """
    To run all tests, run the following command from the root directory:
    >>> poetry run pytest
    """

    def test_restrict_dataset_area(self):
        # Create a test dataset
        lat_min, lat_max, lon_min, lon_max = -2.0, 16.95, 3.0, 9.0
        data_resolution = 0.05

        ds = generate_restrict_dataset_area_test_dataset(
            lat_min, lat_max, lon_min, lon_max, data_resolution
        )

        # Define area of interest
        r_lat_min, r_lat_max, r_lon_min, r_lon_max = -2.0, 17.0, 5.0, 7.0
        buffer_value = data_resolution / 4

        # Restrict the dataset to the area of interest using the function
        restricted_ds = restrict_dataset_area(
            ds, r_lat_min, r_lat_max, r_lon_min, r_lon_max, buffer_value
        )

        # Assert that the restricted dataset boundaries are within the expected ranges
        self.assertGreater(
            restricted_ds.latitude.min().item(), r_lat_min - buffer_value
        )
        self.assertLess(restricted_ds.latitude.max().item(), r_lat_max + buffer_value)
        self.assertGreater(
            restricted_ds.longitude.min().item(), r_lon_min - buffer_value
        )
        self.assertLess(restricted_ds.longitude.max().item(), r_lon_max + buffer_value)

        # Perform strciter test that resulting and expected bounds are (almost) identical
        self.assertAlmostEqual(
            restricted_ds.latitude.min().item(), r_lat_min + data_resolution / 2
        )
        self.assertAlmostEqual(
            restricted_ds.latitude.max().item(), r_lat_max - data_resolution / 2
        )
        self.assertAlmostEqual(
            restricted_ds.longitude.min().item(), r_lon_min + data_resolution / 2
        )
        self.assertAlmostEqual(
            restricted_ds.longitude.max().item(), r_lon_max - data_resolution / 2
        )
