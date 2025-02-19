from src.components.data_ingestion import DataIngestion

from src.exception.exception import FintechException
from src.logging.logger import logging
from src.entity.config_entity import DataIngestionConfig,TrainingPipelineConfig

import sys

if __name__ == '__main__':
    try:
        trainingpipelineconfig = TrainingPipelineConfig()
        dataingestionconfig = DataIngestionConfig(trainingpipelineconfig)
        data_ingestion = DataIngestion(dataingestionconfig)
        logging.info('Initiate the data ingestion')
        dataingestionartifact = data_ingestion.inititate_data_ingestion()
        logging.info('Data Initiation Completed')
        print(dataingestionartifact)

    except Exception as e:
        raise FintechException(e,sys)