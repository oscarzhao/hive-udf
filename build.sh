#!/usr/bin/env bash

rm -rf output
mkdir output
mvn clean package -Dmaven.test.skip=true
mkdir -p output
mv target/hive-udf-1.0.0.jar output/hive-udf-1.0.0.jar
