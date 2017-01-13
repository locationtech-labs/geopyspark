#!/usr/bin/env python3

from pyspark import SparkContext

import tile_test
import multiband_test
import extent_test
import keys_test
import tuple_test
import key_value_test


if __name__ == "__main__":
    sc = SparkContext(master="local", appName="schema-tests")

    tile_test.main(sc)
    multiband_test.main(sc)
    extent_test.main(sc)
    keys_test.main(sc)
    tuple_test.main(sc)
    key_value_test.main(sc)
