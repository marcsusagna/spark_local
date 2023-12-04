from abc import ABC, abstractmethod

from example_library.utils.spark_session import SessionManager
from example_library.utils.storage import StorageManager

class SparkJob(ABC):
    """
    Abstract class to define a Spark Job, i.e. the transformation logic on data pipelines
    """

    def __init__(self, job_name: str, path_to_data_folder: str):
        self.job_name=job_name
        self._prepare_spark_job(path_to_data_folder)
    
    def _prepare_spark_job(self, path_to_data_folder: str) -> None:
        """
        Retrieves Spark Session and configures storage location
        """
        self.session_manager=SessionManager()
        # Make spark_session first class citizen of class SparkJob
        self.spark=self.session_manager.spark
        self.storage_manager=StorageManager(path_to_data_folder)

    @abstractmethod
    def read_inputs(self):
        raise NotImplementedError("To be implemented in subclass")

    @abstractmethod
    def transformation_logic(self):
        raise NotImplementedError("To be implemented in subclass")

    @abstractmethod
    def write_output(self):
        raise NotImplementedError("To be implemented in subclass")

    @abstractmethod
    def main(self):
        raise NotImplementedError("To be implemented in subclass")