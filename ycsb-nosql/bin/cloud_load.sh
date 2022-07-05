#!/bin/bash

source bin/env

bin/ycsb load nosql -cp $CLASSPATH -P ./workloads/workloadc -jvm-args "-Xmx8g -Xms8g -Dsun.net.inetaddr.ttl=0" -s -threads 2 -p nosql.region=ap-seoul-1 -p nosql.oci_config=/home/opc/.oci/config -p nosql.db=nosqltest -p table=jsontable -p recordcount=4096000 -p maxexecutiontime=600000  -p operationcount=1000000000
 
