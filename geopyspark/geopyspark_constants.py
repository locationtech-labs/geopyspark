"""GeoPySpark package constants."""
from os import path

"""GeoPySpark version."""
VERSION = '0.3.0'

"""Backend jar name."""
JAR = 'geotrellis-backend-assembly-' + VERSION + '.jar'

"""The current location of this file."""
CWD = path.abspath(path.dirname(__file__))
