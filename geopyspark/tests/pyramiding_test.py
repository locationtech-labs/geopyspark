import os
import unittest
import rasterio

from geopyspark.geotrellis.constants import SPATIAL
from geopyspark.tests.python_test_utils import check_directory, geotiff_test_path
from geopyspark.geotrellis.geotiff_rdd import get
from geopyspark.tests.base_test_class import BaseTestClass


check_directory()


class PyramidingTest(BaseTestClass):
    dir_path = geotiff_test_path("all-ones.tif")
    rdd = get(BaseTestClass.geopysc, SPATIAL, dir_path)

    def test_pyramid_building(self):
        metadata = self.rdd.collect_metadata(crs="EPSG:4326")

        laid_out = self.rdd.tile_to_layout(metadata)

        result = laid_out.pyramid(start_zoom=1, end_zoom=12)

        previous_layout_cols = None
        previous_layout_rows = None

        for x in result[1:]:
            metadata = x.layer_metadata
            layout_cols = metadata['layoutDefinition']['tileLayout']['layoutCols']
            layout_rows = metadata['layoutDefinition']['tileLayout']['layoutRows']

            if previous_layout_cols and previous_layout_rows:
                self.assertEqual(layout_cols*2, previous_layout_cols)
                self.assertEqual(layout_rows*2, previous_layout_rows)
            else:
                self.assertTrue(layout_cols % 2 == 0)
                self.assertTrue(layout_rows % 2 == 0)

            previous_layout_cols = layout_cols
            previous_layout_rows = layout_rows


if __name__ == "__main__":
    unittest.main()
