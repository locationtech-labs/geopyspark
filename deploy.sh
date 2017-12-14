#!/usr/bin/env bash

set -e
set -x

./sbt "++TRAVIS_SCALA_VERSION" -Dbintray.user=$BINTRAY_USER -Dbintray.pass=$BINTRAY_PASS "project geotrellis-backend" publish
