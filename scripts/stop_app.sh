#!/bin/bash

config=$(cat containers/config.json)
SPARK_CONTAINER_NAME=$(echo $config | jq -r ".CONTAINER_NAME")

# Remove any previously running container
docker stop $SPARK_CONTAINER_NAME
docker rm $SPARK_CONTAINER_NAME