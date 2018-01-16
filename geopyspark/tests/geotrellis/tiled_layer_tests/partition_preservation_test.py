import unittest
import pytest

from geopyspark.geotrellis import SpatialPartitionStrategy, LocalLayout
from geopyspark.geotrellis.constants import LayerType, Operation
from geopyspark.geotrellis.geotiff import get
from geopyspark.geotrellis.neighborhood import Square
from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.tests.python_test_utils import file_path


class PartitionPreservationTest(BaseTestClass):
    rdd = get(LayerType.SPATIAL, file_path("srtm_52_11.tif"), max_tile_size=6001)

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_partition_preservation(self):
        partition_states = []
        strategy = SpatialPartitionStrategy(16)

        tiled = self.rdd.tile_to_layout()

        tiled2 = self.rdd.tile_to_layout(partition_strategy=strategy)
        partition_states.append(tiled2.get_partition_strategy())

        added_layer = (tiled + tiled2) * 0.75
        partition_states.append(added_layer.get_partition_strategy())

        local_max_layer = added_layer.local_max(tiled)
        partition_states.append(local_max_layer.get_partition_strategy())

        focal_layer = local_max_layer.focal(Operation.MAX, Square(1))
        partition_states.append(focal_layer.get_partition_strategy())

        reprojected_layer = focal_layer.tile_to_layout(
            layout=LocalLayout(),
            target_crs=3857,
            partition_strategy=strategy)
        partition_states.append(reprojected_layer.get_partition_strategy())

        pyramided = reprojected_layer.pyramid()
        partition_states.append(pyramided.levels[pyramided.max_zoom].get_partition_strategy())

        self.assertTrue(all(x == partition_states[0] for x in partition_states))


if __name__ == "__main__":
    unittest.main()
