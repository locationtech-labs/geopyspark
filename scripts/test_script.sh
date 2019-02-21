#!/bin/bash

testpath="geopyspark/tests/"
testpattern="*_test.py"

for f in $testpath/**/$testpattern $testpath/**/**/$testpattern;
do
  pytest -v $f || { exit 1; }
done
