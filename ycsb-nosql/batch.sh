#!/bin/bash
./bin/cloud_run-r100-w0.sh 1> r100-w0 2>&1
sleep 60
./bin/cloud_run-r0-w100.sh 1> r0-w100 2>&1
sleep 60
./bin/cloud_run-r50-w50.sh 1> r50-w50 2>&1
sleep 60

