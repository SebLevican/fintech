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