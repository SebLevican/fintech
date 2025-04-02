import os
import sys
import numpy as np
import pandas as pd


PIPELINE_NAME = 'Fintech_data'
ARTIFACT_DIR = 'Artifacts'

DATA_INGESTION_DIR_NAME='data_ingestion'
SCHEMA_FILE_PATH=os.path.join('data_schema','schema.yaml')
DB_NAME = 'credittransactions'

''''
Data Validation
'''
DATA_VALIDATION_DIR_NAME: str ='data_validation'
DATA_VALIDATION_VALID_DIR: str ="validated"
DATA_VALIDATION_INVALID_DIR: str = "invalid"
DATA_VALIDATION_DRIFT_REPORT_DIR: str = 'drift_report'
DATA_VALIDATION_DRIFT_REPORT_FILE_NAME: str = 'report.yaml'
PREPROCESSING_OBJECT_FILE_NAME: str = 'preprocessing.pkl'

'''
Data Transformation
'''

DATA_TABLE_NAME: str = "user_transactions"
DATA_TRANSFORMATION_DIR_NAME: str = 'data_transformation'
DATA_TRANSFORMATION_TRANSFORMED_DATA_DIR: str = 'transformed'
DATA_TRANSFORMATION_TRANSFORMED_OBJECT_DIR: str = 'transformed_object'


