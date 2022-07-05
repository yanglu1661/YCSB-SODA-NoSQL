#!/bin/bash

source bin/env

bin/ycsb run nosql -cp $CLASSPATH -P ./workloads/workloadc2 -jvm-args "-Xmx8g -Xms8g -Dsun.net.inetaddr.ttl=0" -s -threads 20 -p nosql.region=ap-seoul-1 -p nosql.oci_config=/home/opc/.oci/config -p table=jsontable -p recordcount=4096000 -p maxexecutiontime=600  -p operationcount=1000000000
 
