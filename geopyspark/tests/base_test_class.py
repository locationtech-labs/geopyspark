import unittest
import os

from geopyspark.geopycontext import GeoPyContext


class BaseTestClass(unittest.TestCase):
    if 'TRAVIS' in os.environ:
        master_str = "local[1]"
    else:
        master_str = "local[*]"
    geopysc = GeoPyContext(master=master_str, appName="test")
