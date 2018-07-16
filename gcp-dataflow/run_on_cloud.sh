#!/bin/bash

export GOOGLE_APPLICATION_CREDENTIALS=/Users/ajay/Downloads/gcp-credentials-7b19bbb98d97.json

if [ "$#" -lt 3 ]; then
   echo "Usage:   ./run_oncloud.sh project-name bucket-name classname [options] "
   echo "Example: ./run_oncloud.sh cloud-training-demos cloud-training-demos CurrentConditions --bigtable"
   exit
fi

PROJECT=$1
shift
BUCKET=$1
shift
MAIN=com.dilteam.dataflow.$1
shift

echo "Launching $MAIN project=$PROJECT bucket=$BUCKET $*"
echo $PWD

java -cp $PWD/target/gcp-dataflow-0.0.1-SNAPSHOT.jar $MAIN \
  --project=$PROJECT \
  --stagingLocation=gs://$BUCKET/staging/ \
  --tempLocation=gs://$BUCKET/staging/ \
  --numWorkers=4 \
  --runner=DataflowRunner
