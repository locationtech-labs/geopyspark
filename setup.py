from setuptools import setup
import sys

if not sys.version_info[0] >= 3:
    sys.exit("GeoPySpark Does Not Support Python Versions Before 3.0")

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
        'shapely>=1.6b3'
    ],
    include_package_data=True,
    data_files=[('jar', ['geopyspark/jars/geotrellis-backend-assembly-0.1.0.jar'])],
    packages=['geopyspark', 'geopyspark.geotrellis', 'geopyspark.tests'],
    scripts=[],
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Topic :: Scientific/Engineering :: GIS',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ]
)
