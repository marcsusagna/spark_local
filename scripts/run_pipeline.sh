#!/bin/bash

# Arguments passed as environment variables
# SPARK_CONTAINER_NAME


echo "Running actual spark job"
docker exec $CONTAINER_NAME spark-submit --master local[6] --executor-memory 8g ./main.py

# From spark_library
data_path=../data/
spark-submit --master local[6] --executor-memory 8g ./example_library/spark_job_main.py --job top_sessions --data_path $data_path

# From repo root
data_path=data/
spark-submit --master local[6] --executor-memory 8g ./spark_library/example_library/spark_job_main.py --job top_sessions --data_path $data_path

# On container: 
data_path=data/
docker exec $SPARK_CONTAINER_NAME \
    spark-submit --master local[6] --executor-memory 8g ./spark_library/example_library/spark_job_main.py --job top_sessions --data_path $data_path

