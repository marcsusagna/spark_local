import os
import sys

from pyspark.sql import DataFrame


class StorageManager:
    def __init__(self, path_to_data_folder: str, create_data_folders=True):
        self.data_folder=path_to_data_folder
        self._define_data_paths()
        if create_data_folders:
            self._create_data_folders()
    
    def _define_data_paths(self) -> None:
        self.input_dir=self.data_folder
        self.output_dir=f"{self.data_folder}/output/"
        self.analysis_dir=f"{self.data_folder}/analysis/"
    
    def _create_data_folders(self) -> None:
        os.makedirs(self.input_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.analysis_dir, exist_ok=True)
    
    def write_to_disk(self, df: DataFrame, file_name: str, num_output_partitions = 1) -> None:
        """
        For this task it is rather simplistic, but generally there are reused steps when storing to disk.
        To simplify steps, there is no partition column specificed
        :param df:
        :param output_dir:
        :return: None
        """
        df.repartition(num_output_partitions).write.\
            option("header", True).option("delimiter", "\t").mode("overwrite")\
            .csv(f"{self.output_dir}{file_name}.csv")
        # Print query plan for analysis
        sys.stdout = open(f"{self.analysis_dir}/{file_name}.txt", "w")
        df.explain(mode="simple")
        sys.stdout.close()