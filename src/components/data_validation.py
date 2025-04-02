from src.exception.exception import FintechException
from src.logging.logger import logging
from src.constants.training_pipeline import SCHEMA_FILE_PATH
from src.utils.common import read_yaml_file,write_yaml_file
from src.entity.artifact_entity import DataValidationArtifact
from src.entity.config_entity import DataIngestionConfig,DataValidationConfig
import pandas as pd
import sys
import os
from scipy.stats import ks_2samp


class DataValidation:
    def __init__(self,data_validation_artifact:DataValidationArtifact,
                 data_validation_config:DataValidationConfig):
        try:
            self.data_ingestion_artifact=data_validation_artifact
            self.data_validation_config=data_validation_config
            self._schema_config= read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise FintechException(e,sys)
        
    @staticmethod
    def read_data(file_path)->pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise FintechException
        
    def validate_number_of_columns(self,df,dataframe:pd.DataFrame)->bool:
        try:
            number_of_columns=len(self._schema_config[df]['columns'])
            logging.info(f'Required number of columns: {number_of_columns}')
            logging.info(f'Data frame has columns:{len(dataframe.columns)}')
            if len(dataframe.columns) == number_of_columns:
                return True
        except Exception as e:
            raise FintechException(e,sys)
        
    def detect_dataset_drift(self,base_df,current_df,threshold=0.5)->bool:
        try:
            status=True
            report={}
            for column in base_df:
                d1=base_df[column]
                d2=current_df[column]
                is_same_dist=ks_2samp(d1,d2)
                if threshold <= is_same_dist.pvalue:
                    is_found=False
                else:
                    is_found=True
                    status=False
                report.update({column:{
                    'p_value':float(is_same_dist.pvalue),
                    'drift_status':is_found
                }})
            drift_report_file_path=self.data_validation_config.drift_report_file_path

            dir_path = os.path.dirname(drift_report_file_path)
            os.makedirs(dir_path,exist_ok=True)
            write_yaml_file(filep_path=drift_report_file_path,content=report)
        except Exception as e:
            raise FintechException(e,sys)
        
    def clean_columns(self, df:pd.DataFrame)-> pd.DataFrame:
        try:

            columns_transformations ={
            "credit_limit": lambda x: x.str.replace("$", "").str.replace(",", "").astype(float).fillna(0).astype(int),
            "per_capita_income": lambda x: x.str.replace("$", "").str.replace(",", "").str.replace("50K", "50000").fillna("0"),
            "yearly_income": lambda x: x.str.replace("$", "").astype(float).fillna(0).astype(int),
            "total_debt": lambda x: x.str.replace("$", "").astype(float).fillna(0).astype(int),
            "amount": lambda x: x.str.replace("$", "").str.replace(",", "").astype(float).fillna(0).astype(int)
            }

            for col, func in columns_transformations.items():
                if col in df.columns:
                    df[col] = func(df[col])


            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'], errors='coerce')

            return df
        
        except Exception as e:
            raise FintechException(e,sys)



    def initiate_data_validation(self,df_name: str, df: pd.DataFrame) -> DataValidationArtifact:
        try:
            df= self.clean_columns(df)

            status = self.validate_number_of_columns(df_name,df)

            if status:
                logging.info(f'Validation columns sucessful')
            else:
                logging.warning(f'Validation columns error for {df_name}')

            data_validation_artifact = DataValidationArtifact(
                validation_status = status,
                drift_report_file_path = self.data_validation_config.drift_report_file_path,
                dataframes = df
            )
            return data_validation_artifact


        except Exception as e:
            raise FintechException(e,sys)
        
        