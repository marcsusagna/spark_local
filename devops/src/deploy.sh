#!/bin/bash

version=$1

#### Step 3: Continuous Deployment

## Step 3.1: Deploy docker container with application
my_cwd=$(pwd)
docker run -dit --name "$SPARK_CONTAINER_NAME" -v "$my_cwd:/workspace" "$SPARK_IMAGE_NAME:$version"

# Export some variables for using the container interactively
export SPARK_CONTAINER_NAME=$SPARK_CONTAINER_NAME