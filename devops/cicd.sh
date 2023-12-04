#run as 
# devops/cicd.sh 0.0.1

#### Step 1: Continuous Integration

## Step 1.1: Checkout git (no need, working locally, have access to git)

## Step 1.2: Run Unit tests
cd spark_library
pytest -v tests/

## Step 1.3: Build library
# Identify version:
version=$1
python3 setup.py bdist_wheel --version=$version