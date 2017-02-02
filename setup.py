from setuptools import setup
import sys
import os

if not sys.version_info[0] >= 3:
    sys.exit("GeoPySpark Does Not Support Python Versions Before 3.0")

ROOT = os.path.abspath(os.getcwd() + "/")

setup(
        name='geopyspark',
        version='0.1.0',
        author='Jacob Bouffard, James McClain',
        author_email='jbouffard@azavea.com, jmcclain@azavea.com',
        download_url='http://github.com/locationtech-labs/geopyspark',
        description='Python bindings for GeoTrellis and GeoMesa',
        long_description=open('README.md').read(),
        license='LICENSE.txt',
        install_requires=[
            'avro-python3>=1.8',
            'numpy>=1.8',
            'shapely>=1.6b3',
            'rasterio>=0.36.0',
            'py4j>=0.10.4'
            ],
        test_requires=['pytest>=3.0.6'],
        packages=['geopyspark', 'geopyspark.geotrellis', 'geopyspark.tests'],
        scripts=[],
        classifiers=[
            'Development Status :: 2 - Pre-Alpha',
            'License :: OSI Approved :: Apache Software License',
            'Topic :: Scientific/Engineering :: GIS',
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.2',
            'Programming Language :: Python :: 3.3',
            'Programming Language :: Python :: 3.4',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6'
            ]
        )
