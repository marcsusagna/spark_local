#!/bin/bash


### Step 0: Initialization build agent if devops running on docker
# cd devops
# 
# Identify version:
# version=$1
# Start container to run devops
# config=$(cat config.json)
# SPARK_VERSION=$(echo $config | jq -r ".SPARK_VERSION")
# HADOOP_VERSION=$(echo $config | jq -r ".HADOOP_VERSION")
# DEVOPS_IMAGE_NAME=$(echo $config | jq -r ".IMAGE_NAME")
# DEVOPS_IMAGE_VERSION=$(echo $config | jq -r ".VERSION")
# DEVOPS_CONTAINER_NAME=$(echo $config | jq -r ".CONTAINER_NAME")
# 
# docker image build \
    # --build-arg SPARK_VERSION=$SPARK_VERSION \
    # --build-arg HADOOP_VERSION=$HADOOP_VERSION \
    # -t "$DEVOPS_IMAGE_NAME:$DEVOPS_IMAGE_VERSION" \
    # -f Dockerfile_devops .
# 
# Remove any previously running container
# docker stop $DEVOPS_CONTAINER_NAME
# docker rm $DEVOPS_CONTAINER_NAME
# 
# Run container loading volume
# cd .. 
# my_cwd=$(pwd)
# docker run -dit --name "$DEVOPS_CONTAINER_NAME" -v "$my_cwd:/workspace" "$DEVOPS_IMAGE_NAME:$DEVOPS_IMAGE_VERSION"
#  
# docker exec -it $DEVOPS_CONTAINER_NAME /bin/bash

#### Step 0: Initialization build agent assuming running on new VM (no docker for build agents)
cd devops
python3 -m venv venvs/devops
source venvs/devops/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements_devops.txt
cd ..

