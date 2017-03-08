from geopyspark.tests.python_test_utils import *
add_spark_path()
check_directory()

from geopyspark.geotrellis.tile import TileArray
from geopyspark.geotrellis.extent import Extent
from geopyspark.geotrellis.projected_extent import ProjectedExtent
from geopyspark.geotrellis.tile_layer_methods import TileLayerMethods
from geopyspark.geotrellis.geotiff_rdd import HadoopGeoTiffRDD
from geopyspark.tests.base_test_class import BaseTestClass

import unittest
import pytest
import json
import rasterio
import numpy as np


class TileLayerMetadataTest(BaseTestClass):
    metadata = TileLayerMethods(BaseTestClass.geopysc)
    hadoop_geotiff = HadoopGeoTiffRDD(BaseTestClass.geopysc)

    dir_path = geotiff_test_path("all-ones.tif")
    options = {'maxTileSize': 256}

    def check_results(self, actual, expected):
        if isinstance(actual, list) and isinstance(expected, list):
            for x,y in zip(actual, expected):
                self.check_results(x, y)
        elif isinstance(actual, dict) and isinstance(expected, dict):
            self.assertDictEqual(actual, expected)
        else:
            self.assertEqual(actual, expected)

    def test_collection_avro_rdd(self):
        rdd = self.hadoop_geotiff.get_rdd("spatial", "singleband", self.dir_path)
        value = rdd.collect()[0]

        projected_extent = value[0]
        old_extent = projected_extent.extent

        new_extent = {
            "xmin": old_extent.xmin,
            "ymin": old_extent.ymin,
            "xmax": old_extent.xmax,
            "ymax": old_extent.ymax
        }

        (rows, cols) = value[1].shape

        layout = {
            "layoutCols": 1,
            "layoutRows": 1,
            "tileCols": cols,
            "tileRows": rows
        }

        actual = [[new_extent, layout], new_extent]

        result = self.metadata.collect_metadata("spatial",
                                                "singleband",
                                                rdd,
                                                new_extent,
                                                layout,
                                                epsg_code=value[0].epsg_code)

        expected = [[result['layoutDefinition']['extent'],
                     result['layoutDefinition']['tileLayout']],
                    result['extent']]

        self.check_results(actual, expected)

    def test_collection_python_rdd(self):
        data = rasterio.open(self.dir_path)

        old_extent = Extent(data.bounds.left,
                            data.bounds.bottom,
                            data.bounds.right,
                            data.bounds.top)

        new_extent = {
            "xmin": data.bounds.left,
            "ymin": data.bounds.bottom,
            "xmax": data.bounds.right,
            "ymax": data.bounds.top
        }

        (rows, cols) = data.shape

        layout = {
            "layoutCols": 1,
            "layoutRows": 1,
            "tileCols": cols,
            "tileRows": rows
        }

        projected_extent = ProjectedExtent(old_extent, epsg_code=3426)
        tile_dict = {'arr': data.read(1), 'no_data_value': data.nodata}
        rdd = self.geopysc.pysc.parallelize([(projected_extent, tile_dict)])

        actual = [[new_extent, layout], new_extent]

        result = self.metadata.collect_metadata("spatial",
                                                "singleband",
                                                rdd,
                                                new_extent,
                                                layout,
                                                epsg_code=3426)

        expected = [[result['layoutDefinition']['extent'],
                     result['layoutDefinition']['tileLayout']],
                    result['extent']]

        self.check_results(actual, expected)


if __name__ == "__main__":
    unittest.main()
