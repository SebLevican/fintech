import sys
import os
import numpy as np
import pandas as pd
from sqlalchemy import text
import gc
from time import sleep
from src.entity.artifact_entity import (
    DataValidationArtifact,DataTransformationArtifact
)

from src.entity.config_entity import DataTransformationConfig

from src.exception.exception import FintechException
from src.logging.logger import logging
from src.utils.common import users_table,cards_table,transactions_table,merchants_table,date_table,time_table


from conn import engine

class DataTransformation:
    def __init__(self, data_validation_artifact: DataValidationArtifact, data_transformation_config: DataTransformationArtifact):
        try:
            self.data_validation_artifact:DataValidationArtifact=data_validation_artifact
            self.data_transformation_config:DataTransformationConfig=data_transformation_config
        
        except Exception as e:
            raise FintechException(e,sys)
        
    def create_database_conn(self):
        try:
            with engine.connect() as connection:
                connection.execute(text(users_table()))
                connection.execute(text(cards_table()))
                connection.execute(text(merchants_table()))
                connection.execute(text(date_table()))
                connection.execute(text(time_table()))
                connection.execute(text(transactions_table()))
                connection.commit()
        except Exception as e:
            raise FintechException(e, sys)

    def optimize_dataframe(self,df):
        for col in df.select_dtypes(include=['int64']).columns:
            df[col] = df[col].astype('int32')
        for col in df.select_dtypes(include=['float64']).columns:
            df[col] = df[col].astype('float32')
        return df


    def upload_to_db(self, df, engine):
        # 🔹 Extracción de dimensiones optimizadas
        df_users = df['users'].dataframes[[
            "id", "gender", "birth_year", "birth_month", "current_age", 
            "retirement_age", "per_capita_income", "yearly_income", 
            "total_debt", "credit_score", "num_credit_cards"
        ]]
        
        df_cards = df['cards'].dataframes[[
            "id", "client_id", "card_brand", "card_type", "has_chip", 
            "num_cards_issued", "credit_limit", "acct_open_date", 
            "year_pin_last_changed", "card_on_dark_web"
        ]]

        df_merchants = df['transactions'].dataframes[['merchant_id', "merchant_city", "merchant_state", "zip", "mcc", 'id']]
        df_merchants.rename(columns={ 'id': 'transaction_id'}, inplace=True)
        df_merchants = df_merchants.drop_duplicates(subset=['merchant_id'])

        df_dates = df['transactions'].dataframes[["date"]].drop_duplicates()
        df_dates['full_date'] = pd.to_datetime(df_dates['date']).dt.date
        df_dates["year"] = df_dates["date"].dt.year
        df_dates["month"] = df_dates["date"].dt.month
        df_dates["day"] = df_dates["date"].dt.day
        df_dates["quarter"] = df_dates["date"].dt.quarter
        df_dates["week_of_year"] = df_dates["date"].dt.isocalendar().week
        df_dates = df_dates.drop_duplicates(subset=['full_date']).reset_index(drop=True)
        df_dates['id'] = range(1, len(df_dates) + 1)

        df_times = df['transactions'].dataframes[["date"]].drop_duplicates()
        df_times['full_time'] = pd.to_datetime(df_times['date']).dt.time
        df_times['hour'] = pd.to_datetime(df_times['date']).dt.hour
        df_times['minute'] = pd.to_datetime(df_times['date']).dt.minute
        df_times['second'] = pd.to_datetime(df_times['date']).dt.second
        df_times = df_times.reset_index(drop=True)
        

        df_transactions = df['transactions'].dataframes[[
            "id", "client_id", "card_id", "date", "amount", "use_chip", "errors",'merchant_id'
        ]]
        df_transactions['date'] = pd.to_datetime(df_transactions['date'])
        df_transactions['full_date'] = df_transactions['date'].dt.date
        df_transactions['full_time'] = df_transactions['date'].dt.time

        # 🚀 Optimización de DataFrames antes de cargar
        df_users = self.optimize_dataframe(df_users)
        df_cards = self.optimize_dataframe(df_cards)
        df_merchants = self.optimize_dataframe(df_merchants)
        df_dates = self.optimize_dataframe(df_dates)
        df_times = self.optimize_dataframe(df_times)
        df_transactions = self.optimize_dataframe(df_transactions)
        df_times = df_times.drop_duplicates(subset=['full_time'])

        df_times['id'] = range(1, len(df_times) + 1)

        df_times = df_times[['id', 'full_time', 'hour', 'minute', 'second']]
        df_dates = df_dates[['id', 'full_date', 'year', 'month', 'day', 'quarter', 'week_of_year']]

        df_dates.to_sql('dim_date', con=engine, if_exists='append', index=False, chunksize=10_000, method='multi')
        df_times.to_sql('dim_time', con=engine, if_exists='append', index=False, chunksize=10_000, method='multi')
        df_users.to_sql('dim_users', con=engine, if_exists='append', index=False, chunksize=10_000, method='multi')
        df_cards.to_sql('dim_cards', con=engine, if_exists='append', index=False, chunksize=10_000, method='multi')
        df_merchants.to_sql('dim_merchants', con=engine, if_exists='append', index=False, chunksize=10_000, method='multi')

        # 🔥 Procesar en chunks para evitar MemoryError
        chunk_size = 50_000  # Reducido para menor consumo de RAM
        id_counter = 1 
        for chunk in np.array_split(df_transactions, len(df_transactions) // chunk_size + 1):
            chunk = chunk.merge(df_dates, on="full_date", how="left")
            chunk.rename(columns={"id_y": "date_id"}, inplace=True)
            chunk = chunk.merge(df_times, on="full_time", how="left")


            chunk.rename(columns={"id": "time_id"}, inplace=True)  # id de df_times

            # 3. Ajustar merchant_id 
            chunk.rename(columns={"id_x": "transactions_id"}, inplace=True)
            chunk['id'] = range(id_counter, id_counter + len(chunk))
            id_counter += len(chunk)  # Actualizar contador global
            print(chunk.dtypes)  # Verifica los tipos de datos
            print(chunk.head(3).to_dict())  # Muestra un pequeño ejemplo de los datos
            chunk = chunk[["id", "client_id", "card_id", "merchant_id", "date_id", "time_id", "amount", "use_chip", "errors"]]

            print(chunk.dtypes)  # Verifica los tipos de datos
            print(chunk.head(3).to_dict())  # Muestra un pequeño ejemplo de los datos

            existing_ids = pd.read_sql("SELECT id FROM fact_transactions", con=engine)

            chunk_new = chunk[~chunk['id'].isin(existing_ids['id'])]

            # Insertar solo los nuevos registros
            chunk_new.to_sql('fact_transactions', con=engine, if_exists='append', index=False, chunksize=10_000, method='multi')

            #sleep(430)
            # Insertar directamente en SQL sin guardar en memoria
            #chunk.to_sql('fact_transactions', con=engine, if_exists='append', index=False, chunksize=10_000, method='multi')

            del chunk
            gc.collect()  # Liberar memoria

        # 🔹 Renombrar columnas correctamente y limpiar datos


        # 🔥 Carga de datos en SQL en chunks más pequeños
        try:
            #df_dates.to_sql('dim_date', con=engine, if_exists='append', index=False, chunksize=10_000, method='multi')
            #df_times.to_sql('dim_time', con=engine, if_exists='append', index=False, chunksize=10_000, method='multi')
            return {
                "users": df_users,
                "cards": df_cards,
                "merchants": df_merchants,
                "dates": df_dates,
                "times": df_times,
                "transactions": df_transactions
            }
        except Exception as e:
            logging.error(f"Error en la ejecución de la consulta: {e}", exc_info=True)
            raise FintechException(e, sys)


        
        
    def initiate_data_transformation(self,df) -> DataTransformationArtifact:
        logging.info('initiate data transformation and upload to db')
        try:
            logging.info('Create db if are need it')
            self.create_database_conn()
            logging.info('Database created')
            logging.info('uploading to db')
            data_transformation_artifact = DataTransformationArtifact(self.upload_to_db(df,engine))
            
            return data_transformation_artifact

        except Exception as e:
            raise FintechException(e,sys)


        # Asegurar que 'errors' sea booleano
        #df_transactions["errors"] = df_transactions["errors"].notna()
