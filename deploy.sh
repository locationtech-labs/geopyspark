#!/usr/bin/env bash

set -e
set -x

aws s3 cp geopyspark/jars/geotrellis-backend-assembly-*.jar s3://geopyspark-dependency-jars/ --acl public-read \
  && cd geopyspark-backend \
  && ./sbt -Dbintray.user=$BINTRAY_USER -Dbintray.pass=$BINTRAY_PASS "project geotrellis-backend" publish \
  && ./sbt -Dbintray.user=$BINTRAY_USER -Dbintray.pass=$BINTRAY_PASS "project vectorpipe" publish \
  && ./sbt -Dbintray.user=$BINTRAY_USER -Dbintray.pass=$BINTRAY_PASS "project geotools" publish
