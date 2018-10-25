import os
import unittest
import rasterio
import numpy as np
import pytest

from geopyspark import geotiff
from geopyspark.geotrellis import (Extent,
                                   ProjectedExtent,
                                   TileLayout,
                                   Tile,
                                   LayoutDefinition,
                                   GlobalLayout,
                                   LocalLayout,
                                   SpatialPartitionStrategy,
                                   catalog)
from geopyspark.geotrellis.constants import LayerType
from geopyspark.geotrellis.layer import Pyramid, RasterLayer
from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.tests.python_test_utils import file_path


class PyramidingTest(BaseTestClass):

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_pyraminding_with_partitioner(self):
        arr = np.zeros((1, 16, 16))
        epsg_code = 3857
        extent = Extent(0.0, 0.0, 10.0, 10.0)

        tile = Tile(arr, 'FLOAT', False)
        projected_extent = ProjectedExtent(extent, epsg_code)

        rdd = BaseTestClass.pysc.parallelize([(projected_extent, tile)])
        raster_rdd = RasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd)
        tile_layout = TileLayout(32, 32, 16, 16)
        new_extent = Extent(-20037508.342789244, -20037508.342789244, 20037508.342789244,
                            20037508.342789244)

        layout_def = LayoutDefinition(new_extent, tile_layout)
        laid_out = raster_rdd.tile_to_layout(GlobalLayout(tile_size=16))

        strategy = SpatialPartitionStrategy(4)

        pyramided = laid_out.pyramid(partition_strategy=strategy)

        self.assertEqual(pyramided.levels[0].get_partition_strategy(), strategy)

    def test_correct_base(self):
        arr = np.zeros((1, 16, 16))
        epsg_code = 3857
        extent = Extent(0.0, 0.0, 10.0, 10.0)

        tile = Tile(arr, 'FLOAT', False)
        projected_extent = ProjectedExtent(extent, epsg_code)

        rdd = BaseTestClass.pysc.parallelize([(projected_extent, tile)])
        raster_rdd = RasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd)
        tile_layout = TileLayout(32, 32, 16, 16)
        new_extent = Extent(-20037508.342789244, -20037508.342789244, 20037508.342789244,
                            20037508.342789244)
        layout_def = LayoutDefinition(new_extent, tile_layout)

        laid_out = raster_rdd.tile_to_layout(GlobalLayout(tile_size=16))
        result = laid_out.pyramid()
        self.pyramid_building_check(result)

    # collect_metadata needs to be updated for this to work
    '''
    def test_no_start_zoom(self):
        arr = np.zeros((1, 16, 16))
        epsg_code = 3857
        extent = Extent(0.0, 0.0, 10.0, 10.0)

        tile = Tile(arr, 'FLOAT', None)
        projected_extent = ProjectedExtent(extent, epsg_code)

        rdd = BaseTestClass.pysc.parallelize([(projected_extent, tile)])
        raster_rdd = RasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd)
        tile_layout = TileLayout(32, 32, 16, 16)
        new_extent = Extent(-20037508.342789244, -20037508.342789244, 20037508.342789244,
                            20037508.342789244)

        layout_def = LayoutDefinition(new_extent, tile_layout)
        metadata = raster_rdd.collect_metadata(layout=layout_def)
        laid_out = raster_rdd.tile_to_layout(metadata)
        reprojected = laid_out.reproject(3857, layout=GlobalLayout(zoom=laid_out.zoom_level))

        result = reprojected.pyramid(end_zoom=1)

        self.pyramid_building_check(result)
    '''

    def test_local_pyramid(self):
        arr = np.zeros((1, 256, 256))
        epsg_code = 3857
        extent = Extent(0.0, 0.0, 10.0, 10.0)

        tile = Tile(arr, 'FLOAT', None)
        projected_extent = ProjectedExtent(extent, epsg_code)

        rdd = BaseTestClass.pysc.parallelize([(projected_extent, tile)])

        raster_rdd = RasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd)
        laid_out = raster_rdd.tile_to_layout(LocalLayout(256))

        # Single tile is at level 0
        result = laid_out.pyramid()
        assert result.max_zoom == 0

        laid_out = raster_rdd.tile_to_layout(LocalLayout(16))
        result = laid_out.pyramid()

        assert result.max_zoom == 4
        assert result.levels[4].layer_metadata.tile_layout.layoutCols == 16
        assert result.levels[3].layer_metadata.tile_layout.layoutCols == 8
        assert result.levels[2].layer_metadata.tile_layout.layoutCols == 4
        assert result.levels[1].layer_metadata.tile_layout.layoutCols == 2
        assert result.levels[0].layer_metadata.tile_layout.layoutCols == 1

    def test_pyramid_class(self):
        arr = np.zeros((1, 16, 16))
        epsg_code = 3857
        extent = Extent(0.0, 0.0, 10.0, 10.0)

        tile = Tile(arr, 'FLOAT', False)
        projected_extent = ProjectedExtent(extent, epsg_code)

        rdd = BaseTestClass.pysc.parallelize([(projected_extent, tile)])
        raster_rdd = RasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd)
        tile_layout = TileLayout(1, 1, 16, 16)
        reprojected = raster_rdd.tile_to_layout(layout=GlobalLayout(tile_size=16), target_crs=3857)

        result = reprojected.pyramid()
        hist = result.get_histogram()

        self.assertEqual(result.max_zoom, reprojected.zoom_level)
        self.assertTrue(set(result.levels.keys()).issuperset(range(1, 13)))
        self.assertEqual(hist.mean(), 0.0)
        self.assertEqual(hist.min_max(), (0.0, 0.0))

    def pyramid_building_check(self, result):
        previous_layout_cols = None
        previous_layout_rows = None

        values = sorted(list(result.levels.items()), reverse=True, key=lambda tup: tup[0])

        for x in values:
            metadata = x[1].layer_metadata
            layout_cols = metadata.tile_layout.layoutCols
            layout_rows = metadata.tile_layout.layoutRows

            if previous_layout_cols and previous_layout_rows:
                self.assertEqual(layout_cols*2, previous_layout_cols)
                self.assertEqual(layout_rows*2, previous_layout_rows)
            else:
                self.assertTrue(layout_cols % 2 == 0)
                self.assertTrue(layout_rows % 2 == 0)

            previous_layout_cols = layout_cols
            previous_layout_rows = layout_rows

    def test_write_pyramid_layers(self):
        max_zoom = 5
        tif = file_path('srtm_52_11.tif')
        raster_layer = geotiff.get(layer_type=LayerType.SPATIAL, uri=tif)
        tiled_raster_layer = raster_layer.tile_to_layout(GlobalLayout(zoom=max_zoom), target_crs=3857)
        pyramided_layer = tiled_raster_layer.pyramid()

        layer_name = 'pyramid-test-layer'
        path = file_path('pyramid-test-catalog')
        uri = 'file:///' + path

        if os.path.isdir(path):
            import shutil
            shutil.rmtree(path)

        pyramided_layer.write(uri, layer_name)

        self.assertTrue(catalog.read_layer_metadata(uri, layer_name, 0))
        self.assertTrue(catalog.read_layer_metadata(uri, layer_name, max_zoom))

if __name__ == "__main__":
    unittest.main()
