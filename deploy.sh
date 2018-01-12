#!/usr/bin/env bash

set -e
set -x

cd geopyspark-backend && ./sbt -Dbintray.user=$BINTRAY_USER -Dbintray.pass=$BINTRAY_PASS "project geotrellis-backend" publish
cd geopyspark-backend && ./sbt -Dbintray.user=$BINTRAY_USER -Dbintray.pass=$BINTRAY_PASS "project vectorpipe" publish
