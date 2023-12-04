#!/bin/bash

config=$(cat containers/config.json)
SPARK_IMAGE_NAME=$(echo $config | jq -r ".IMAGE_NAME")
SPARK_CONTAINER_NAME=$(echo $config | jq -r ".CONTAINER_NAME")

# Remove any previously running container
docker stop $SPARK_CONTAINER_NAME
docker rm $SPARK_CONTAINER_NAME

source devops/src/deploy.sh latest

# Export some variables for using the container interactively
export CONTAINER_NAME=$CONTAINER_NAME