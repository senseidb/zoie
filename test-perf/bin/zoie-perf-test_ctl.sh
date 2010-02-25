#!/bin/bash
# need JAVA_HOME set to find the java compiler
JAVA_HOME=${JAVA_HOME:-/usr/java} ;  export JAVA_HOME

APPNAME=zoie-perf-test
SVNTAG=$1

BASEDIR=`dirname $0`/..
BASEDIR=`(cd $BASEDIR && pwd)`
echo $BASEDIR

cd $BASEDIR

ADD_CLASSPATH=$JAVA_HOME/lib/tools.jar
for file in lib/*.jar
do
  ADD_CLASSPATH=$ADD_CLASSPATH:$file
done

for file in lib/*.zip
do
  ADD_CLASSPATH=$ADD_CLASSPATH:$file
done

for file in $(find lib -name "*.jar")
do
  ADD_CLASSPATH=$ADD_CLASSPATH:$file
done

for file in $(find lib -name "*.zip")
do
  ADD_CLASSPATH=$ADD_CLASSPATH:$file
done

ADD_CLASSPATH=$ADD_CLASSPATH:conf:resource
echo $ADD_CLASSPATH
while test "$#" -gt 0
do
  case "$1" in
  -debug)	     JAVA_DEBUG_FLAGS="${JAVA_DEBUG_FLAGS} -Xrunjdwp:transport=dt_socket,address=8002,server=y,suspend=y";;
	
	esac
  shift
done

export JAVA_DEBUG_FLAGS

java -d64 ${JAVA_DEBUG_FLAGS} -Xms1000m -Xmx1000m -cp $ADD_CLASSPATH \
-Dperf.env=${APPNAME} -Dperf.svntag=${SVNTAG} -Dzoie.perf.env=zoieperftest -Dlog4j.configuration=file://$BASEDIR/conf/log4j.xml  \
org.deepak.performance.LoadRunner
