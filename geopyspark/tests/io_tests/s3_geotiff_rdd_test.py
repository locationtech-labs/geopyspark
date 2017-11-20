from os import walk, path
import unittest
import rasterio
import pytest
import numpy as np
from geopyspark.geotrellis.constants import LayerType
from geopyspark.geotrellis import LocalLayout, SpatialKey
from geopyspark.tests.python_test_utils import geotiff_test_path
from geopyspark.geotrellis.geotiff import get
from geopyspark.tests.base_test_class import BaseTestClass


class S3GeoTiffIOTest(object):
    def assertTilesEqual(self, a, b):
        """compare two numpy arrays that are tiles"""
        self.assertEqual(a.shape, b.shape)
        cmp = (a == b)  # result must be array of matching cells
        diff = np.argwhere(cmp == False)
        if np.size(diff) > 0:
            raise Exception("Tiles differ at: ", np.size(diff), diff)
        return True

    def get_filepaths(self, dir_path):
        files = []

        for (fd, dn, filenames) in walk(dir_path):
            files.extend(filenames)

        return [path.join(dir_path, x) for x in files]

    def read_geotiff_rasterio(self, paths, windowed):
        rasterio_tiles = []

        windows = [((0, 256), (0, 256)),
                   ((256, 512), (0, 256)),
                   ((0, 256), (256, 512)),
                   ((256, 512), (256, 512))]

        for f in paths:
            with rasterio.open(f) as src:
                if not windowed:
                    rasterio_tiles.append({'cells': src.read(),
                                           'no_data_value': src.nodata})
                else:
                    for window in windows:
                        rasterio_tiles.append(
                            {'cells': src.read(window=window),
                             'no_data_value': src.nodata})

        return rasterio_tiles


class Multiband(S3GeoTiffIOTest, BaseTestClass):
    mock_wrapper = BaseTestClass.pysc._gateway.jvm.geopyspark.geotrellis.testkit.MockS3ClientWrapper
    client = mock_wrapper.mockClient()

    key = "one-month-tiles-multiband/result.tif"
    bucket = "test"

    uri = "s3://test/one-month-tiles-multiband/result.tif"
    file_path = geotiff_test_path(key)
    options = {"s3Client": "mock"}

    in_file = open(file_path, "rb")
    cells = in_file.read()
    in_file.close()

    @pytest.fixture(scope='class', autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def read_multiband_geotrellis(self, opt=options):
        self.client.putObject(self.bucket, self.key, self.cells)
        result = get(LayerType.SPATIAL,
                     self.uri,
                     s3_client=opt['s3Client'],
                     max_tile_size=opt.get('maxTileSize'))

        return result

    def test_segment_tiles(self):
        # GeoTrellis will read GeoTiff Segments given no window size
        # Retile them to match Rasterio read and check the cell values
        geotrellis_tiles = self.read_multiband_geotrellis()\
                               .tile_to_layout(LocalLayout(512))\
                               .to_numpy_rdd().collect()
        # TODO: assert there is only one geotrellis tile
        geotrellis_tile = dict(geotrellis_tiles)[SpatialKey(0,0)]

        rasterio_tiles = self.read_geotiff_rasterio([self.file_path], False)
        self.assertEquals(len(rasterio_tiles), 1)
        rasterio_tile = rasterio_tiles[0]

        self.assertTilesEqual(geotrellis_tile.cells, rasterio_tile['cells'])

    def windowed_result_checker(self, windowed_tiles):
        self.assertEqual(len(windowed_tiles), 4)

    def test_windowed_tiles(self):
        geotrellis_tiles = self.read_multiband_geotrellis({"s3Client": "mock", "maxTileSize": 256})
        geotrellis_tiles = geotrellis_tiles.to_numpy_rdd().values().collect()
        sorted_1 = sorted(geotrellis_tiles, key=lambda x: x.cells[0, 0, 0])

        rasterio_tiles = self.read_geotiff_rasterio([self.file_path], True)
        sorted_2 = sorted(rasterio_tiles, key=lambda x: x['cells'][0, 0, 0])

        self.windowed_result_checker(geotrellis_tiles)

        for x, y in zip(sorted_1, sorted_2):
            print('\n')
            print('This is read in from geotrellis', x.cells.shape)
            print('This is read in from rasterio', y['cells'].shape)
            self.assertTrue(np.array_equal(x.cells, y['cells']))


if __name__ == "__main__":
    unittest.main()
