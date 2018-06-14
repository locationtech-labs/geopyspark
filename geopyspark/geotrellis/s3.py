"""Utilities relating to Amazon S3"""

from collections import namedtuple
from contextlib import contextmanager

from geopyspark import get_spark_context


__all__ = ['Credentials']


S3A_FS_CONSTANT = 'org.apache.hadoop.fs.s3a.S3AFileSystem'
S3A_IMPL_PATH = 'spark.hadoop.fs.s3a.impl'

_S3_ACCESS_KEY_PATH_TEMPLATE = 'spark.hadoop.fs.{prefix}.access.key'
_S3_SECRET_KEY_PATH_TEMPLATE = 'spark.hadoop.fs.{prefix}.secret.key'
_S3_URI_PREFIXES = frozenset(['s3', 's3a', 's3n'])


Credentials = namedtuple(
    'Credentials',
    ['access_key', 'secret_key']
)
Credentials.__doc__ = (
    """Credentials for Amazon S3 buckets.

    Attributes:
        access_key (str): The access key for the S3 bucket.
        secret_key (str): The secret key for the S3 bucket.
    """
)

@contextmanager
def set_s3_credentials(credentials, uri_type):
    """Temporarily updates the session's Amazon S3 credentials for the
       duration of the context.

    Args:
        credentials (Credentials): The access and secret keys used to access
            Amazon S3 resources.
        uri_type (str): The URI type. 's3', 's3a', or 's3n'.
    """
    if credentials:
        if uri_type not in _S3_URI_PREFIXES:
            raise RuntimeError(
                'Cannot set S3 credentials for unrecognized URI type '
                '{}'.format(uri_type)
            )
        configuration = get_spark_context()._conf
        with _set_s3_credentials(credentials, configuration, uri_type):
            yield
    else:
        yield


@contextmanager
def _set_s3_credentials(credentials, configuration, uri_type):
    access_key_path = _S3_ACCESS_KEY_PATH_TEMPLATE.format(prefix=uri_type)
    secret_key_path = _S3_SECRET_KEY_PATH_TEMPLATE.format(prefix=uri_type)

    old_s3_access_key = configuration.get(access_key_path)
    old_s3_secret_key = configuration.get(secret_key_path)

    try:
        configuration.set(access_key_path, credentials.access_key)
        configuration.set(secret_key_path, credentials.secret_key)
        if uri_type == 's3a':
            with _set_s3a_path(configuration):
                yield
        else:
            yield
    finally:
        configuration.set(access_key_path, old_s3_access_key)
        configuration.set(secret_key_path, old_s3_secret_key)


@contextmanager
def _set_s3a_path(configuration):
    old_s3a_impl = configuration.get(S3A_IMPL_PATH)
    try:
        configuration.set(S3A_IMPL_PATH, S3A_FS_CONSTANT)
        yield
    finally:
        configuration.set(S3A_IMPL_PATH, old_s3a_impl)


def is_s3_uri(uri):
    """Determines if the specified ``uri`` points to Amazon S3.

    Args:
        uri (str): The Universal Resource Identifier to examine.

    Returns:
        bool: True if ``uri`` is an Amazon S3 URI.
    """
    return any(
        uri.startswith(prefix + '://') for prefix in _S3_URI_PREFIXES
    )
