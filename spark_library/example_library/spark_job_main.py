# Library entrypoint called as
# run_spark_job --job path.to.spark_job where path.to_spark_job is relative within transformations folder
# When developing locally, you can run it as python3 example_library/spark_job_main.py --job path.to.spark_job

import os
import sys
import argparse
from importlib import import_module

current_dir=os.path.dirname(os.path.abspath(__file__))
library_dir=os.path.abspath(os.path.join(current_dir, ".."))
sys.path.insert(0, library_dir)


def main() -> None:
    # Parse arguments
    parser=argparse.ArgumentParser(description="Spark job parser")
    parser.add_argument("--job", required=True, help="path to the job int he transformations folder")
    parser.add_argument("--data_path", required=True, help="path to data folder")
    args=parser.parse_args()
    job=args.job
    data_path=args.data_path

    # Load transformation logic
    module_path=f"example_library.transformations.{job}"
    job_module=import_module(module_path)
    job_class=getattr(job_module, "CLASS_NAME")
    job_name=getattr(job_module, "JOB_NAME")
    my_job_class=getattr(job_module, job_class)
    my_job=my_job_class(job_name, data_path)
    # Run transformation
    my_job.main()


if __name__=="__main__":
    print(f"current_dir: {current_dir}")
    main()