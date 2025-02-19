import os
import sys
import pandas as pd
from typing import List

from src.exception.exception import FintechException
from src.logging.logger import logging
from src.entity.config_entity import DataIngestionConfig, TrainingPipelineConfig
from src.entity.artifact_entity import DataIngestionArtifact

class DataIngestion:
    def __init__(self,data_ingestion_config:DataIngestionConfig):
        try:
            self.data_ingestion_config=data_ingestion_config
        except Exception as e:
            raise FintechException(e,sys)
        
    def read_dataframes(self):
        try:
            self.cards = pd.read_csv(self.data_ingestion_config.files['cards'])
            self.transactions = pd.read_csv(self.data_ingestion_config.files['transactions'])
            self.users = pd.read_csv(self.data_ingestion_config.files['users'])

            print('Files loaded')

            return {
                'cards': self.cards,
                'transactions': self.transactions,
                'users': self.users
            }
        except Exception as e:
            raise FintechException(e,sys)
        
    def clean_dfs(self):
        try:
            dataframes = self.read_dataframes()
            cards = dataframes['cards']
            print(cards.head())
        except Exception as e:
            raise FintechException(e,sys)
        
    def inititate_data_ingestion(self):
        try:
            training_pipeline_config = TrainingPipelineConfig()
            data_ingestion_config = DataIngestionConfig(training_pipeline_config)

            # Crear instancia de DataIngestion
            data_ingestion = DataIngestion(data_ingestion_config)

            # Cargar DataFrames
            dataframes = data_ingestion.read_dataframes()
            dataingestionartifact= DataIngestionArtifact(dataframes)
            return dataingestionartifact
        except Exception as e:
            raise FintechException(e,sys)