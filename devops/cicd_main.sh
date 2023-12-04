#!/bin/bash

#run as 
# devops/cicd.sh 0.0.1
# where the argument is the version to build and deploy

version=$1

# Step 0: Init build agent to run devops pipeline
source devops/src/init_build_agent.sh

# Step 1: Build and publish
source devops/src/build_and_publish.sh $version

# Step 2: Deploy app in a container
source devops/src/deploy.sh $version





