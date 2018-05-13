"""Unit tests for the s3 module"""

from unittest import mock

import pytest

from geopyspark.geotrellis import s3


@pytest.fixture(autouse=True, scope='function')
def delete_fake_configuration():
    yield
    FakeConfiguration().clear()


class FakeConfiguration:

    _config = {}

    def __init__(self):
        self._data = FakeConfiguration._config

    def clear(self):
        FakeConfiguration._config.clear()

    def __getattr__(self, name):
        if name == '_conf':
            return self
        raise AttributeError

    def set(self, key, value):
        self._data[key] = value

    def get(self, key):
        try:
            return self._data[key]
        except KeyError:
            return ''


@mock.patch(
    'geopyspark.geotrellis.s3.get_spark_context',
    return_value=FakeConfiguration()
)
@pytest.mark.parametrize('s3_uri_type', ['s3', 's3a', 's3n'])
def test_set_s3_credentials(_fake_config, s3_uri_type):
    fake_config = FakeConfiguration()
    access_key_path = 'spark.hadoop.fs.' + s3_uri_type + '.access.key'
    secret_key_path = 'spark.hadoop.fs.' + s3_uri_type + '.secret.key'

    fake_config.set(s3.S3A_IMPL_PATH, 'impl')
    fake_config.set(access_key_path, 'access')
    fake_config.set(secret_key_path, 'secret')

    sample_credentials = s3.Credentials('new_access', 'new_secret')

    with s3.set_s3_credentials(sample_credentials, s3_uri_type):
        assert (
            fake_config.get(s3.S3A_IMPL_PATH) == s3.S3A_FS_CONSTANT
            if s3_uri_type == 's3a'
            else 'impl'
        )
        assert fake_config.get(access_key_path) == 'new_access'
        assert fake_config.get(secret_key_path) == 'new_secret'

    assert fake_config.get(s3.S3A_IMPL_PATH) == 'impl'
    assert fake_config.get(access_key_path) == 'access'
    assert fake_config.get(secret_key_path) == 'secret'


def test_set_s3_credentials_with_bad_protocol():
    sample_credentials = s3.Credentials('new_access', 'new_secret')

    with pytest.raises(RuntimeError):
        with s3.set_s3_credentials(sample_credentials, 's3z'):
            pass


@mock.patch(
    'geopyspark.geotrellis.s3.get_spark_context',
    return_value=FakeConfiguration()
)
def test_set_empty_credentials(*_args):
    fake_config = FakeConfiguration()

    fake_config.set(s3.S3N_ACCESS_KEY_PATH, 'access')
    fake_config.set(s3.S3N_SECRET_KEY_PATH, 'secret')

    with s3.set_s3_credentials(None, 's3n'):
        assert fake_config.get(s3.S3N_ACCESS_KEY_PATH) == 'access'
        assert fake_config.get(s3.S3N_SECRET_KEY_PATH) == 'secret'


@pytest.mark.parametrize('uri, expected_result', [
    ('s3n://something', True),
    ('s3a://something_else', True),
    ('s3://yet_another_thing', True),
    ('https://something_i_want', False)
])
def test_is_s3_uri(uri, expected_result):
    assert s3.is_s3_uri(uri) == expected_result
