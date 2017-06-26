#!/usr/bin/env bash

USERID=$1
GROUPID=$2
JAR=$3

unzip /archives/s3+hdfs.zip
cd thredds-feature-s3-hdfs
./gradlew assemble
cp build/libs/$JAR /archives

chown -R $USERID:$GROUPID /archives ~/.m2 ~/.gradle
