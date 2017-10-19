import pathlib
import tempfile
import rasterio
import unittest
import pytest

from geopyspark.geotrellis import Tile
from geopyspark.geotrellis.constants import LayerType
from geopyspark.geotrellis.geotiff import get
from geopyspark.tests.python_test_utils import geotiff_test_path
from geopyspark.tests.base_test_class import BaseTestClass


class ToGeoTiffTest(BaseTestClass):
    dir_path = geotiff_test_path("all-ones.tif")
    rdd = get(LayerType.SPATIAL, dir_path, max_tile_size=1024)
    metadata = rdd.collect_metadata()

    mapped_types = {
        'int8': 'BYTE',
        'uint8': 'UBYTE',
        'int16': 'SHORT',
        'uint16': 'USHORT',
        'int32': 'INT',
        'float': 'FLOAT',
        'float32': 'FLOAT',
        'double': 'DOUBLE'
    }

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_to_geotiff_rdd_rasterlayer(self):
        geotiff_rdd = self.rdd.to_geotiff_rdd(storage_method="Tiled",
                                              compression="DeflateCompression",
                                              color_space=0,
                                              head_tags={'INTERLEAVE': 'BAND'})

        geotiff_bytes = geotiff_rdd.first()[1]

        with tempfile.NamedTemporaryFile() as temp:
            temp.write(geotiff_bytes)
            temp_path = pathlib.Path(temp.name)

            with rasterio.open(str(temp_path)) as src:
                self.assertTrue(src.is_tiled)

                profile = src.profile

                self.assertEqual(profile['blockxsize'], 256)
                self.assertEqual(profile['blockysize'], 256)
                self.assertEqual(profile['interleave'], 'band')
                self.assertEqual(src.compression, rasterio.enums.Compression.deflate)

    def test_to_geotiff_rdd_tiledrasterlayer(self):
        tiled_rdd = self.rdd.tile_to_layout()
        tiled_collected = tiled_rdd.to_numpy_rdd().first()[1]

        geotiff_rdd = tiled_rdd.to_geotiff_rdd()
        geotiff_collected = geotiff_rdd.first()[1]

        def to_geotiff(x):
            with tempfile.NamedTemporaryFile() as temp:
                temp.write(x)
                temp_path = pathlib.Path(temp.name)

                with rasterio.open(str(temp_path)) as src:
                    self.assertFalse(src.is_tiled)
                    data = src.read()
                    return Tile(data, self.mapped_types[str(data.dtype)], src.nodata)

        rasterio_geotiff = to_geotiff(geotiff_collected)

        self.assertTrue((tiled_collected.cells == rasterio_geotiff.cells).all())
        self.assertEqual(tiled_collected.cell_type, rasterio_geotiff.cell_type)
        self.assertEqual(tiled_collected.no_data_value, rasterio_geotiff.no_data_value)


if __name__ == "__main__":
    unittest.main()
