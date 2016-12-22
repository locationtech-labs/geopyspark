from distutils.core import setup

setup(
        description='geopyspark',
        author='Jacob Bouffard',
        download_url='http://github.com/geotrellis/geopyspark',
        author_email='jbouffard@azavea.com',
        version='0.1',
        install_requires=['py4j, pyspark'],
        packages=['geopyspark'],
        scripts=[],
        name='geopyspark'
        )
