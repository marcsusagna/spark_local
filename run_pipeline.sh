#!/bin/bash

# Arguments passed as environment variables
# CONTAINER_NAME


echo "Running data quality checks"
docker exec $CONTAINER_NAME spark-submit --master local[4] --executor-memory 2g ./data_health_checks_main.py

echo "Running actual spark job"
docker exec $CONTAINER_NAME spark-submit --master local[4] --executor-memory 2g ./main.py