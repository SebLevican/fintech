import os
import sys

from src.exception.exception import FintechException
from src.logging.logger import logging

from src.components.data_ingestion import DataIngestion
from src.components.data_validation import DataValidation

from src.entity.config_entity import DataIngestionConfig,TrainingPipelineConfig,DataValidationConfig

from src.entity.artifact_entity import DataIngestionArtifact,DataValidationArtifact


class TrainingPipeline:
    def __init__(self):
        self.training_pipeline_config = TrainingPipelineConfig()

    def start_data_ingestion(self):
        try:
            self.data_ingestion_config = DataIngestionConfig(training_pipeline_config=self.training_pipeline_config)
            logging.info('Start data Ingestion')
            data_ingestion=DataIngestion(data_ingestion_config=self.data_ingestion_config)
            data_ingestion_artifact=data_ingestion.inititate_data_ingestion()
            logging.info(f'Data Ingestion completed and artifact: {data_ingestion_artifact}')
        except Exception as r:
            raise FintechException(r,sys)
        
    def start_data_validation(self,data_ingestion_artifact:DataIngestionArtifact):
        try:
            data_validation_config=DataValidationConfig(training_pipeline_config=self.training_pipeline_config)
            data_validation=DataValidation(data_ingestion_artifact=data_ingestion_artifact,data_validation_config=data_validation_config)
            logging.info('Initiate the data Validation')
            data_validation_artifact=data_validation.initiate_data_validation()
        


    def run_pipeline(self):
        try:
            data_ingestion_artifact=self.start_data_ingestion()
            data_validation_artifact=self.start_data

        except Exception as e:
            raise FintechException(e,sys)