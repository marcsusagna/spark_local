#!/bin/bash

# job_to_run: Name of the python file in example_library/transformations that you want to run

job_to_run=$1

data_path=data/
spark-submit --master local[6] --executor-memory 8g $(which run_spark_job.py) --job $job_to_run --data_path $data_path