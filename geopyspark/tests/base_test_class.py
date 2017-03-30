import unittest

from geopyspark.geopycontext import GeoPyContext


class BaseTestClass(unittest.TestCase):
    geopysc = GeoPyContext(master="local[1]", appName="test")
