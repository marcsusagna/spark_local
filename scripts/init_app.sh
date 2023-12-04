#!/bin/bash

version=$1

config=$(cat containers/config.json)
SPARK_IMAGE_NAME=$(echo $config | jq -r ".IMAGE_NAME")
SPARK_CONTAINER_NAME=$(echo $config | jq -r ".CONTAINER_NAME")

source devops/src/deploy.sh $version

# Export some variables for using the container interactively
export CONTAINER_NAME=$CONTAINER_NAME