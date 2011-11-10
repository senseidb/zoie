#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

cd $bin/..
mvn -e exec:java -Dexec.mainClass=proj.zoie.perf.client.ZoiePerf
