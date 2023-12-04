from pyspark.sql import SparkSession


class SessionManager:
    def __init__(self):
        self.spark= (
            SparkSession
            .builder
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("OFF")