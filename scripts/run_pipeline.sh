#!/bin/bash

# job_to_run: Name of the python file in example_library/transformations that you want to run

job_to_run=$1

SPARK_CONTAINER_NAME=spark_container

docker exec $SPARK_CONTAINER_NAME /bin/bash -c "bash scripts/pipeline_main.sh $job_to_run"