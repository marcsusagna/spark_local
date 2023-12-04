#!/bin/bash

# Arguments passed as environment variables
# SPARK_CONTAINER_NAME

# job_to_run: Name of the python file in example_library/transformations that you want to run

job_to_run=$1

docker exec $SPARK_CONTAINER_NAME /bin/bash -c "bash scripts/pipeline_main.sh $job_to_run"