#!/bin/bash

source bin/env

bin/ycsb run nosql -cp $CLASSPATH -P ./workloads/workloada -jvm-args "-Xmx32g -Xms32g -Dsun.net.inetaddr.ttl=0" -s -threads 100 -p nosql.url=mysqlx://10.1.1.161:33060 -p nosql.user=admin -p nosql.password=Oracle123456! -p nosql.db=nosqltest -p table=usertable2 -p recordcount=4096000 -p maxexecutiontime=600  -p operationcount=1000000000
 
