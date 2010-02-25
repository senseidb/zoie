#!/bin/bash
# need JAVA_HOME set to find the java compiler
JAVA_HOME=${JAVA_HOME:-/usr/java} ;  export JAVA_HOME

APPNAME=manage-load-result

BASEDIR=`dirname $0`/..
BASEDIR=`(cd $BASEDIR && pwd)`
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

ADD_CLASSPATH=$ADD_CLASSPATH:conf:components

while test "$#" -gt 0
do
  case "$1" in
  -debug)            JAVA_DEBUG_FLAGS="${JAVA_DEBUG_FLAGS} -Xrunjdwp:transport=dt_socket,address=8002,server=y,suspend=y";;
      * )        CMDLINEPARAM="${CMDLINEPARAM} $1"
        esac
  shift
done


java -Xms512m -Xmx1g -cp $ADD_CLASSPATH \
org.deepak.performance.LoadResultManager $CMDLINEPARAM
