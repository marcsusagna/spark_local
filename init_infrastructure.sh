#!/bin/bash

IMAGE_VERSION="0.0.1"
IMAGE_NAME="spark_local"
CONTAINER_NAME=spark_container

# Obtain config values:
config=$(cat containers/config.json)
SPARK_VERSION=$(echo $config | jq -r ".SPARK_VERSION")
HADOOP_VERSION=$(echo $config | jq -r ".HADOOP_VERSION")

# Build docker image
docker image build \
    --build-arg SPARK_VERSION=$SPARK_VERSION \
    --build-arg HADOOP_VERSION=$HADOOP_VERSION \
    -t "$IMAGE_NAME:$IMAGE_VERSION" \
    -f containers/Dockerfile_spark .

# Remove any previously running container
docker stop $CONTAINER_NAME
docker rm $CONTAINER_NAME

# Run container loading volume
my_cwd=$(pwd)
docker run -dit --name "$CONTAINER_NAME" -v "$my_cwd:/workspace" "$IMAGE_NAME:$IMAGE_VERSION"

# Export some variables for using the container interactively
export CONTAINER_NAME=$CONTAINER_NAME