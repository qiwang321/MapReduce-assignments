#!/bin/sh
DIR=$(dirname $0)
hadoop jar $DIR/dist/cloud9-1.4.6-1.jar edu.umd.cloud9.example.simple.DemoWordCount -libjars $DIR/dist/cloud9-1.4.6-1.jar -input bible+shakes.nopunc.gz -output qiwang321 -numReducers 5
