from setuptools import setup
import sys

if not sys.version_info[0] >= 3:
    sys.exit("Python Versions Before 3.0 Are Not Supported")

setup(
        description='geopyspark',
        author='Jacob Bouffard',
        download_url='http://github.com/geotrellis/geopyspark',
        author_email='jbouffard@azavea.com',
        version='0.1',
        install_requires=[
            'avro-python3>=1.8',
            'numpy>=1',
            'shapely>=1.6b3',
            'rasterio>=0.36.0'
            ],
        packages=['geopyspark'],
        scripts=[],
        name='geopyspark'
        )
