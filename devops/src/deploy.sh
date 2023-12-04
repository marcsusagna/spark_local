#!/bin/bash

version=$1

#### Step 3: Continuous Deployment

# Step 3.2: Stop running application in PROD
docker stop $SPARK_CONTAINER_NAME
docker rm $SPARK_CONTAINER_NAME

## Step 3.2: Deploy docker container with application
my_cwd=$(pwd)
docker run -p 4040:4040 -dit --name "$SPARK_CONTAINER_NAME" -v "$my_cwd:/workspace" "$SPARK_IMAGE_NAME:$version"

# Export some variables for using the container interactively
export SPARK_CONTAINER_NAME=$SPARK_CONTAINER_NAME