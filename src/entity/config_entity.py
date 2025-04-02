from datetime import datetime
import os
from src.constants import training_pipeline


class TrainingPipelineConfig:
    def __init__(self,timestamp=datetime.now()):
        timestamp =timestamp.strftime('%m_%d_%Y_%H_%M_S')
        self.pipeline_name=training_pipeline.PIPELINE_NAME
        self.artifact_name=training_pipeline.ARTIFACT_DIR
        self.artifact_dir=os.path.join(self.artifact_name,timestamp)
        self.model_dir=os.path.join('final_model')
        self.timestamp: str=timestamp


class DataIngestionConfig:
    def __init__(self,training_pipeline_config:TrainingPipelineConfig):
        self.data_ingestion_dir=os.path.join(
            training_pipeline_config.artifact_dir,training_pipeline.DATA_INGESTION_DIR_NAME
        )
        self.files = {
            "cards": "gs://banks-transaction/cards_dirty.csv",
            "transactions": "gs://banks-transaction/transactions_part_0.csv",
            "users": "gs://banks-transaction/users_dirty.csv"
        }

class DataValidationConfig:
    def __init__(self,training_pipeline_config:TrainingPipelineConfig):
        self.data_validation_dir: str=os.path.join(training_pipeline_config.artifact_dir, training_pipeline.DATA_VALIDATION_DIR_NAME)
        self.valid_data_dir:str= os.path.join(self.data_validation_dir, training_pipeline.DATA_VALIDATION_VALID_DIR)
        self.invalid_data_dir:str= os.path.join(self.data_validation_dir,training_pipeline.DATA_VALIDATION_INVALID_DIR)
        self.drift_report_file_path:str =os.path.join(self.data_validation_dir, training_pipeline.DATA_VALIDATION_DRIFT_REPORT_DIR,training_pipeline.DATA_VALIDATION_DRIFT_REPORT_FILE_NAME)

class DataTransformationConfig:
    def __init__(self,training_pipeline_config:TrainingPipelineConfig):
        self.data_transformation: str= os.path.join(training_pipeline_config.artifact_dir, training_pipeline.DATA_TRANSFORMATION_DIR_NAME)
        self.table_name: str= training_pipeline.DATA_TABLE_NAME
        self.data_transformation_transformed_data_dir:str = training_pipeline.DATA_TRANSFORMATION_TRANSFORMED_DATA_DIR
        self.data_transformation_transformed_object_dir:str = training_pipeline.DATA_TRANSFORMATION_TRANSFORMED_OBJECT_DIR

        

