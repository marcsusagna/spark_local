#!/bin/bash

config=$(cat containers/config.json)
SPARK_CONTAINER_NAME=$(echo $config | jq -r ".CONTAINER_NAME")

# Remove any previously running container
docker stop $CONTAINER_NAME
docker rm $CONTAINER_NAME