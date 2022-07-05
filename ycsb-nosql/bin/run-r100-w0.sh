#!/bin/bash

source bin/env

bin/ycsb run nosql -cp $CLASSPATH -P ./workloads/workloadc -jvm-args "-Xmx8g -Xms8g -Dsun.net.inetaddr.ttl=0" -s -threads 10 -p nosql.url=http://10.1.0.156:5500 -p nosql.db=nosqltest -p table=usertable2 -p recordcount=4096000 -p maxexecutiontime=600  -p operationcount=1000000000
 
