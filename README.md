# spark_local
Playground to develop Spark clusters and CICD around data pipelines built with python libraries

The example pipeline takes the played sessions from http://ocelma.net/MusicRecommendationDataset/lastfm-1K.html and obtains the top 10 songs played in the top 50 longest sessions by tracks count. A session is all played songs sequentially where each song is started within 20 minutes of the previous song's start time. 

## Requirements

Your devops runner should have docker installed as well as the packages described in devops/Dockerfile_devops

## Running the logic

1. Get the data by running: bash scripts/get_data.sh
2. Run the CICD pipeline to deploy the app (otw you won't have the built image). Semantic_version is something like 0.0.1: bash devops/cicd_main.sh {semantic_version}
3. Run the data pipeline with: bash scripts/run_pipeline.sh job_to_run  (job_to_run is job in the transformations module of the library, for instance: bash scripts/run_pipeline.sh top_tracks)
4. Stop the container with: bash scripts/stop_app.sh

If you wish to rerun the app:
1. Start the app again with: bash scripts/init_app.sh {semantic_version}

## Developing new functionality 
1. Create a new branch, onboard new functionality. For instance if a new data pipeline is built, you just need to create a new subclass of SparkJob under the folder transformations. For that you create a new python file with a class and constants CLASS_NAME and JOB_NAME defined (use hello_world.py as inspiration)
2. Run the CICD pipeline to deploy the app: bash devops/src/cicd_main.sh 0.0.2
3. Run any new logic you want to by modifying scripts/run_pipeline.sh job_to_run. Note that you only need to modify the argument --job pointing to the new job
