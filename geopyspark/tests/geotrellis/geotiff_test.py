"""Miscellaneous geotiff tests"""

import pytest

from geopyspark.geotrellis.constants import LayerType
from geopyspark.geotrellis.geotiff import get
from geopyspark.geotrellis.s3 import Credentials
from geopyspark.tests.python_test_utils import file_path


def test_s3_uri_type_and_credential_mismatch():
    local_path = file_path("all-ones.tif")

    with pytest.raises(RuntimeError):
        get(
            LayerType.SPATIAL,
            local_path,
            max_tile_size=256,
            s3_credentials=Credentials('123', 'abc')
        )
