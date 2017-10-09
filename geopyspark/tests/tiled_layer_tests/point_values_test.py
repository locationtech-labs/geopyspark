import datetime
import numpy as np
import pytest
import unittest

from shapely.geometry import Point

from geopyspark.geotrellis import (SpatialKey, SpaceTimeKey, Extent,
                                   Tile, _convert_to_unix_time)
from geopyspark.geotrellis.layer import TiledRasterLayer
from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.geotrellis.constants import LayerType, Operation, ResampleMethod


class PointValuesTest(BaseTestClass):
    first = np.zeros((1, 4, 4))
    first.fill(1)

    second = np.zeros((1, 4, 4))
    second.fill(2)

    extent = {'xmin': 0.0, 'ymin': 0.0, 'xmax': 4.0, 'ymax': 4.0}
    layout = {'layoutCols': 1, 'layoutRows': 1, 'tileCols': 4, 'tileRows': 4}

    now = datetime.datetime.strptime("2017-09-25T11:37:00Z", '%Y-%m-%dT%H:%M:%SZ')

    points = [
        Point(1.0, -3.0),
        Point(2.0, 4.0),
        Point(3.0, 3.0),
        Point(1.0, -2.0),
        Point(-10.0, 15.0)
    ]

    labeled_points = {
        'A': points[0],
        'B': points[1],
        'C': points[2],
        'D': points[3],
        'E': points[4]
    }

    expected_spatial_points_list = [
        (Point(1.0, -3.0), [1, 2]),
        (Point(2.0, 4.0), [1, 2]),
        (Point(3.0, 3.0), [1, 2]),
        (Point(1.0, -2.0), [1, 2]),
        (Point(-10.0, 15.0), None)
    ]

    expected_spacetime_points_list = [
        (Point(1.0, -3.0), now, [1, 2]),
        (Point(2.0, 4.0), now, [1, 2]),
        (Point(3.0, 3.0), now, [1, 2]),
        (Point(1.0, -2.0), now, [1, 2]),
        (Point(-10.0, 15.0), None, None)
    ]

    expected_spatial_points_dict = {
        'A': expected_spatial_points_list[0],
        'B': expected_spatial_points_list[1],
        'C': expected_spatial_points_list[2],
        'D': expected_spatial_points_list[3],
        'E': expected_spatial_points_list[4]
    }

    expected_spacetime_points_dict = {
        'A': expected_spacetime_points_list[0],
        'B': expected_spacetime_points_list[1],
        'C': expected_spacetime_points_list[2],
        'D': expected_spacetime_points_list[3],
        'E': expected_spacetime_points_list[4]
    }

    def create_spatial_layer(self):
        cells = np.array([self.first, self.second], dtype='int')
        tile = Tile.from_numpy_array(cells, -1)

        layer = [(SpatialKey(0, 0), tile),
                 (SpatialKey(1, 0), tile),
                 (SpatialKey(0, 1), tile),
                 (SpatialKey(1, 1), tile)]

        rdd = BaseTestClass.pysc.parallelize(layer)

        metadata = {'cellType': 'int32ud-1',
                    'extent': self.extent,
                    'crs': '+proj=longlat +datum=WGS84 +no_defs ',
                    'bounds': {
                        'minKey': {'col': 0, 'row': 0},
                        'maxKey': {'col': 1, 'row': 1}},
                    'layoutDefinition': {
                        'extent': self.extent,
                        'tileLayout': self.layout
                    }
                   }

        return TiledRasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd, metadata)

    def create_spacetime_layer(self):
        cells = np.array([self.first, self.second], dtype='int')
        tile = Tile.from_numpy_array(cells, -1)

        layer = [(SpaceTimeKey(0, 0, self.now), tile),
                 (SpaceTimeKey(1, 0, self.now), tile),
                 (SpaceTimeKey(0, 1, self.now), tile),
                 (SpaceTimeKey(1, 1, self.now), tile)]

        rdd = BaseTestClass.pysc.parallelize(layer)

        metadata = {'cellType': 'int32ud-1',
                    'extent': self.extent,
                    'crs': '+proj=longlat +datum=WGS84 +no_defs ',
                    'bounds': {
                        'minKey': {'col': 0, 'row': 0, 'instant': _convert_to_unix_time(self.now)},
                        'maxKey': {'col': 1, 'row': 1, 'instant': _convert_to_unix_time(self.now)}
                    },
                    'layoutDefinition': {
                        'extent': self.extent,
                        'tileLayout': self.layout
                    }
                   }

        return TiledRasterLayer.from_numpy_rdd(LayerType.SPACETIME, rdd, metadata)

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_spatial_list_no_resample(self):
        result = self.create_spatial_layer().get_point_values(self.points)

        self.assertEqual(len(result), len(self.expected_spatial_points_list))

        for r in result:
            self.assertTrue(r in self.expected_spatial_points_list)

    def test_spatial_list_with_resample(self):
        result = self.create_spatial_layer().get_point_values(self.points,
                                                              ResampleMethod.NEAREST_NEIGHBOR)

        self.assertEqual(len(result), len(self.expected_spatial_points_list))

        for r in result:
            self.assertTrue(r in self.expected_spatial_points_list)

    def test_spatial_dict_no_resample(self):
        result = self.create_spatial_layer().get_point_values(self.labeled_points)

        self.assertEqual(len(result), len(self.expected_spatial_points_dict))

        for r in result:
            self.assertTrue(r in self.expected_spatial_points_dict)

    def test_spatial_dict_with_resample(self):
        result = self.create_spatial_layer().get_point_values(self.labeled_points,
                                                              ResampleMethod.NEAREST_NEIGHBOR)

        self.assertEqual(len(result), len(self.expected_spatial_points_dict))

        for r in result:
            self.assertTrue(r in self.expected_spatial_points_dict)

    # SpaceTime tests

    def test_spacetime_list_no_resample(self):
        result = self.create_spacetime_layer().get_point_values(self.points)

        self.assertEqual(len(result), len(self.expected_spacetime_points_list))

        for r in result:
            self.assertTrue(r in self.expected_spacetime_points_list)

    def test_spacetime_list_with_resample(self):
        result = self.create_spacetime_layer().get_point_values(self.points,
                                                                ResampleMethod.NEAREST_NEIGHBOR)

        self.assertEqual(len(result), len(self.expected_spacetime_points_list))

        for r in result:
            self.assertTrue(r in self.expected_spacetime_points_list)

    def test_spacetime_dict_no_resample(self):
        result = self.create_spacetime_layer().get_point_values(self.labeled_points)

        self.assertEqual(len(result), len(self.expected_spacetime_points_dict))

        keys = self.expected_spacetime_points_dict.keys()
        values = self.expected_spacetime_points_dict.values()

        for key, value in result.items():
            self.assertTrue(key in keys)
            self.assertTrue(value in values)

    def test_spacetime_dict_with_resample(self):
        result = self.create_spacetime_layer().get_point_values(self.labeled_points,
                                                                ResampleMethod.NEAREST_NEIGHBOR)

        self.assertEqual(len(result), len(self.expected_spacetime_points_dict))

        keys = self.expected_spacetime_points_dict.keys()
        values = self.expected_spacetime_points_dict.values()

        for key, value in result.items():
            self.assertTrue(key in keys)
            self.assertTrue(value in values)


if __name__ == "__main__":
    unittest.main()
    BaseTestClass.pysc.stop()
