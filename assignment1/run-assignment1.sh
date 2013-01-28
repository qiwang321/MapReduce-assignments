cd $( dirname "$BASH_SOURCE[0]")
hadoop jar ./dist/cloud9-1.4.6-1.jar edu.umd.cloud9.example.simple.DemoWordCount -libjars ./dist/cloud9-1.4.6-1.jar -input bible+shakes.nopunc.gz -output qiwang321 -numReducers 5
