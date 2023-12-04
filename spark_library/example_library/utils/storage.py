import os

from pyspark.sql import DataFrame


class StorageManager:
    def __init__(self, data_folder: str):
        self.data_folder=data_folder
        self._define_data_paths()
        self._create_data_folders()
    
    def _define_data_paths(self) -> None:
        self.input_dir=self.data_folder
        self.output_dir=f"{self.data_folder}/output/"
    
    def _create_data_folders(self) -> None:
        os.makedirs(self.input_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
    
    def write_to_disk(df: DataFrame, file_name: str, num_output_partitions = 1) -> None:
        """
        For this task it is rather simplistic, but generally there are reused steps when storing to disk.
        To simplify steps, there is no partition column specificed
        :param df:
        :param output_dir:
        :return: None
        """
        df.repartition(num_output_partitions).write.\
            option("header", True).option("delimiter", "\t").mode("overwrite")\
            .csv(f"{self.output_dir}{file_name}")