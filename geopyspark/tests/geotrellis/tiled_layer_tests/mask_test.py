import os
import unittest
import pytest
import rasterio
import numpy as np

from geopyspark.geotrellis import SpatialKey, Tile, SpatialPartitionStrategy, RasterizerOptions, Metadata
from shapely.geometry import Polygon, box
from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.geotrellis.layer import TiledRasterLayer
from geopyspark.geotrellis.constants import LayerType


class MaskTest(BaseTestClass):
    pysc = BaseTestClass.pysc
    cells = np.zeros((1, 2, 2))
    cells.fill(1)

    layer = [(SpatialKey(0, 0), Tile(cells, 'FLOAT', -1.0)),
             (SpatialKey(1, 0), Tile(cells, 'FLOAT', -1.0,)),
             (SpatialKey(0, 1), Tile(cells, 'FLOAT', -1.0,)),
             (SpatialKey(1, 1), Tile(cells, 'FLOAT', -1.0,))]

    rdd = pysc.parallelize(layer)

    extent = {'xmin': 0.0, 'ymin': 0.0, 'xmax': 4.0, 'ymax': 4.0}
    layout = {'layoutCols': 2, 'layoutRows': 2, 'tileCols': 2, 'tileRows': 2}
    metadata = {'cellType': 'float32ud-1.0',
                'extent': extent,
                'crs': 4326,
                'bounds': {
                    'minKey': {'col': 0, 'row': 0},
                    'maxKey': {'col': 1, 'row': 1}},
                'layoutDefinition': {
                    'extent': extent,
                    'tileLayout': layout}}

    geoms = [box(0.0, 0.0, 2.0, 2.0), box(3.0, 3.0, 4.0, 4.0)]
    raster_rdd = TiledRasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd, Metadata.from_dict(metadata))

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_geotrellis_mask(self):
        result = self.raster_rdd.mask(geometries=self.geoms).to_numpy_rdd()
        n = result.map(lambda kv: np.sum(kv[1].cells)).reduce(lambda a, b: a + b)

        self.assertEqual(n, 2.0)

    def test_rdd_mask_no_partition_strategy(self):
        rdd = BaseTestClass.pysc.parallelize(self.geoms)

        result = self.raster_rdd.mask(rdd, options=RasterizerOptions(True, 'PixelIsArea')).to_numpy_rdd()
        n = result.map(lambda kv: np.sum(kv[1].cells)).reduce(lambda a, b: a + b)

        self.assertEqual(n, 2.0)

    def test_rdd_mask_with_partition_strategy(self):
        rdd = BaseTestClass.pysc.parallelize(self.geoms)

        result = self.raster_rdd.mask(rdd, partition_strategy=SpatialPartitionStrategy()).to_numpy_rdd()
        n = result.map(lambda kv: np.sum(kv[1].cells)).reduce(lambda a, b: a + b)

        self.assertEqual(n, 2.0)

if __name__ == "__main__":
    unittest.main()
