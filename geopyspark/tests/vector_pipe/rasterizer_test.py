import unittest
import pytest

from geopyspark.geotrellis import CellType, SpatialPartitionStrategy
from geopyspark.vector_pipe import osm_reader, Feature, CellValue
from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.tests.python_test_utils import file_path

from geopyspark.geotrellis.rasterize import rasterize_features


class RasterizationTest(BaseTestClass):

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_rasterization(self):
        features = osm_reader.from_orc(file_path("zerns.orc"))

        lines = features.get_way_features_rdd()

        def assign_cellvalues(feature):
            tags = feature.properties.tags.values()

            if 'water' in tags:
                return Feature(feature.geometry, CellValue(4, 4))
            elif "en:Zern's Farmer's Market" in tags:
                return Feature(feature.geometry, CellValue(3, 3))
            else:
                return Feature(feature.geometry, CellValue(1, 1))

        mapped_lines = lines.map(lambda feature: assign_cellvalues(feature))

        result = rasterize_features(mapped_lines, 4326, 12, cell_type=CellType.INT8)

        self.assertEqual(result.get_min_max(), (1, 4))
        self.assertEqual(result.count(), 1)

    def test_rasterization_with_partitioner(self):
        features = osm_reader.from_orc(file_path("zerns.orc"))

        lines = features.get_way_features_rdd()

        def assign_cellvalues(feature):
            tags = feature.properties.tags.values()

            if 'water' in tags:
                return Feature(feature.geometry, CellValue(4, 4))
            elif "en:Zern's Farmer's Market" in tags:
                return Feature(feature.geometry, CellValue(3, 3))
            else:
                return Feature(feature.geometry, CellValue(1, 1))

        mapped_lines = lines.map(lambda feature: assign_cellvalues(feature))

        result = rasterize_features(mapped_lines,
                                    4326,
                                    12,
                                    cell_type=CellType.INT8,
                                    partition_strategy=SpatialPartitionStrategy())

        self.assertEqual(result.get_min_max(), (1, 4))
        self.assertEqual(result.count(), 1)


if __name__ == "__main__":
    unittest.main()
