# Spark Job to be used as example on how to develop Spark logic

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame

from example_library.utils.spark_job import SparkJob

CLASS_NAME="HelloWorld"
JOB_NAME="hello_world"

class HelloWorld(SparkJob):
    def __init__(self, job_name: str):
        super().__init__(job_name)
    
    def _create_inputs(self) -> DataFrame:
        schema = T.StructType(
            [
                T.StructField("message", T.StringType(), True)
            ]
        )
        data=[("Hello World",)]
        df = self.spark.createDataFrame(data, schema=schema)
        return df
    
    def read_inputs(self) -> DataFrame:
        return self._create_inputs()
    
    def transformation_logic(self, source_df: DataFrame):
        source_df = (
            source_df
            .withColumn("message_2", F.lit("Gruezi"))
        )  
        return source_df
    
    def write_output(self, input_df: DataFrame):
        input_df.show()
    
    def main(self):
        input_df=self.read_inputs()
        final_df=self.transformation_logic(input_df)
        self.write_output(final_df)