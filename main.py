from src.components.data_ingestion import DataIngestion
from src.components.data_validation import DataValidation
from src.components.data_transformation import DataTransformation

from src.exception.exception import FintechException
from src.logging.logger import logging
from src.entity.config_entity import DataIngestionConfig, TrainingPipelineConfig, DataValidationConfig, DataTransformationConfig

import sys
import requests  # Para enviar los datos a la API
import json
import os
# URL del servidor FastAPI (ajústala si es necesario)


# Diccionario para almacenar estadísticas del pipeline
pipeline_stats = {}

if __name__ == '__main__':
    try:
        trainingpipelineconfig = TrainingPipelineConfig()
        dataingestionconfig = DataIngestionConfig(trainingpipelineconfig)
        data_ingestion = DataIngestion(dataingestionconfig)
        
        logging.info('✅ Iniciando Data Ingestion...')
        dataingestionartifact = data_ingestion.inititate_data_ingestion()
        
        for df_name, df in dataingestionartifact.files.items():
            pipeline_stats[df_name] = {"ingestion": len(df)}

        logging.info('✅ Data Ingestion Completada')

        with open('stats.json','w') as file:
            json.dump(pipeline_stats, file, indent=4)

        data_validation_config = DataValidationConfig(trainingpipelineconfig)
        data_validation = DataValidation(dataingestionconfig, data_validation_config)
        
        logging.info('✅ Iniciando Data Validation...')
        data_validation_artifacts = {}
        stats_file = 'stats.json'

        if os.path.exists(stats_file):
            with open(stats_file,'r') as file:
                pipeline_stats = json.load(file)

        else:
            pipeline_stats = {}

        for df_name, df in dataingestionartifact.files.items():
            logging.info(f'🔍 Validando {df_name}')

            validation_artifact = data_validation.initiate_data_validation(df_name, df)
            data_validation_artifacts[df_name] = validation_artifact
            print(validation_artifact)
            logging.info(f"Tipo de validation_artifact: {type(validation_artifact)}")
            logging.info(f"Contenido de validation_artifact: {validation_artifact}")
            logging.info(f"Contenido de 'dataframes': {validation_artifact.dataframes}")
            
            # Actualizar stats
            pipeline_stats[df_name]["validation"] = len(df)


        logging.info('✅ Data Validation Completada')




        with open(stats_file,'w') as file:
            json.dump(pipeline_stats, file, indent=4)

        logging.info('✅ Iniciando Data Transformation...')
        data_transformation_config = DataTransformationConfig(trainingpipelineconfig)
        data_transformation = DataTransformation(data_validation_config, data_transformation_config)
        data_transformation_artifact = data_transformation.initiate_data_transformation(data_validation_artifacts)
        logging.info(f"Contenido de data_transformation_artifact después de la transformación: {data_transformation_artifact}")

        logging.info(f"Atributos de data_transformation_artifact: {dir(data_transformation_artifact)}")
        logging.info(f"Contenido de data_transformation_artifact: {data_transformation_artifact}")

        logging.info(data_transformation_artifact.dataframes.keys())

        stats_file = 'stats.json'

        if os.path.exists(stats_file):
            with open(stats_file,'r') as file:
                pipeline_stats = json.load(file)

        else:
            pipeline_stats = {}
        
        for df_name, df in data_transformation_artifact.dataframes.items():
            if df_name not in pipeline_stats:
                pipeline_stats[df_name] = {}  # Crea la entrada si no existe

            pipeline_stats[df_name]["transformation"] = len(df)

        logging.info(pipeline_stats)
        logging.info('✅ Data Transformation Completada')

        # Enviar actualización a FastAPI
        with open('stats.json','w') as file:
            json.dump(pipeline_stats, file, indent=4)

    except Exception as e:
        logging.error(f"Error en la ejecución de la consulta: {e}", exc_info=True)
        raise FintechException(e, sys)
