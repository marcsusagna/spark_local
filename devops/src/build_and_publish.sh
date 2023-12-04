#!/bin/bash

version=$1

#### Step 1: Continuous Integration

## Step 1.1: Checkout git (no need, working locally, have access to git)

## Step 1.2: Run Unit tests
cd spark_library
pytest -v tests/

## Step 1.3: Build library

# Build whl
python3 setup.py bdist_wheel --version=$version

# Move library to dropzone to be uploaded to Artifactory
whl_name=$(ls dist/)
mv dist/* ../devops/artifacts/library

## Step 1.4: Build Docker image

cd ..

config=$(cat containers/config.json)
SPARK_VERSION=$(echo $config | jq -r ".SPARK_VERSION")
HADOOP_VERSION=$(echo $config | jq -r ".HADOOP_VERSION")
SPARK_IMAGE_NAME=$(echo $config | jq -r ".IMAGE_NAME")
SPARK_CONTAINER_NAME=$(echo $config | jq -r ".CONTAINER_NAME")

# Build docker image
docker image build \
    --build-arg SPARK_VERSION=$SPARK_VERSION \
    --build-arg HADOOP_VERSION=$HADOOP_VERSION \
    --build-arg LIBRARY_VERSION=$version \
    -t "$SPARK_IMAGE_NAME:$version" \
    -f containers/Dockerfile_spark . 

#### Step 2: Continuous delivery

## Step 2.1: Publish library to artifactory

## Step 2.2: Publish docker image to Artifactory
