import sys
from pathlib import Path
from setuptools import setup, find_packages

# Create version argument
number_of_arguments = len(sys.argv)
version_parameter=sys.argv[-1]
version=version_parameter.split("=")[1]
sys.argv=sys.argv[0:number_of_arguments-1]

print(f"Version being built: {version}")

with open(Path(__file__).with_name("requirements.txt")) as f:
    required=f.read().splitlines()

setup(
    name="example_library",
    version=version,
    python_requires=">3.10",
    packages=find_packages(),
    install_requires=required,
    entry_points={
        "console_scripts": ["run_spark_job.py = example_library.spark_job_main:main"]
    },
    author="Marc Susagna",
    description="Example on a Spark library"
)