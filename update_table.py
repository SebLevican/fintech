

from src.exception.exception import FintechException
from src.logging.logger import logging
from google.cloud import storage
import pandas as pd
from conn import engine
import numpy as np
import gc
from time import sleep
import sys


class update_db:
    def __init__(self):
        pass

    def listar_archivos(self, bucket_name,prefix):
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blobs = client.list_blobs(bucket, prefix=prefix)

        return [blob.name for blob in blobs]

    def optimize_dataframe(self,df):
        for col in df.select_dtypes(include=['int64']).columns:
            df[col] = df[col].astype('int32')
        for col in df.select_dtypes(include=['float64']).columns:
            df[col] = df[col].astype('float32')
        return df
    
    def check_bucket(self, bucket_name, file_name):
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_name)

        content = blob.download_as_text()
        df = pd.read_csv(pd.io.common.StringIO(content))

        return df
    
    def add_new_data(self,df):
        try:
        
            df_merchants = df[['merchant_id', "merchant_city", "merchant_state", "zip", "mcc", 'id']]
            df_merchants.rename(columns={ 'id': 'transaction_id'}, inplace=True)

            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df_dates = df[["date"]].drop_duplicates()
            df_dates['full_date'] = pd.to_datetime(df_dates['date']).dt.date
            df_dates["year"] = df_dates["date"].dt.year
            df_dates["month"] = df_dates["date"].dt.month
            df_dates["day"] = df_dates["date"].dt.day
            df_dates["quarter"] = df_dates["date"].dt.quarter
            df_dates["week_of_year"] = df_dates["date"].dt.isocalendar().week
            df_dates = df_dates.drop_duplicates(subset=['full_date']).reset_index(drop=True)


            df_times = df[["date"]].drop_duplicates()
            df_times['full_time'] = pd.to_datetime(df_times['date']).dt.time
            df_times['hour'] = pd.to_datetime(df_times['date']).dt.hour
            df_times['minute'] = pd.to_datetime(df_times['date']).dt.minute
            df_times['second'] = pd.to_datetime(df_times['date']).dt.second
            df_times = df_times.reset_index(drop=True)
            

            df_transactions = df[[
                "id", "client_id", "card_id", "date", "amount", "use_chip", "errors"
            ]]
            df_transactions['date'] = pd.to_datetime(df_transactions['date'])
            df_transactions['full_date'] = df_transactions['date'].dt.date
            df_transactions['full_time'] = df_transactions['date'].dt.time

            # 🚀 Optimización de DataFrames antes de cargar
            df_merchants = self.optimize_dataframe(df_merchants)
            df_dates = self.optimize_dataframe(df_dates)
            df_times = self.optimize_dataframe(df_times)
            df_transactions = self.optimize_dataframe(df_transactions)
            df_times = df_times.drop_duplicates(subset=['full_time'])

            #df_times['id'] = range(1, len(df_times) + 1)

            df_times = df_times[[ 'full_time', 'hour', 'minute', 'second']]
            merchants = pd.read_sql('select merchant_id from dim_merchants', con=engine)
            time = pd.read_sql('select full_time from dim_time', con=engine)
            date = pd.read_sql('select full_date from dim_date', con=engine)

            df_merchants['merchant_id'] = df_merchants['merchant_id'].astype(int)
            merchants['merchant_id'] = merchants['merchant_id'].astype(int)
            df_merchants = df_merchants.dropna(subset=['merchant_id'])
            merchants = merchants.dropna(subset=['merchant_id'])


            new_merchants = df_merchants[~df_merchants['merchant_id'].isin(merchants['merchant_id'].values)]

            new_time = df_times[~df_times['full_time'].isin(time['full_time'])]
            new_date = df_dates[~df_dates['full_date'].isin(date['full_date'])]

            new_date = new_date.dropna(how='all')
            new_time = new_time.dropna(how='all')

            new_date = new_date[['full_date', 'year', 'month', 'day', 'quarter', 'week_of_year']]
            new_time = new_time[['full_time', 'hour', 'minute', 'second']]

            new_merchants = new_merchants.drop_duplicates(subset=['merchant_id'])

            new_date.to_sql('dim_date',con=engine, if_exists='append',index=False,chunksize=10_000,method='multi')
            new_time.to_sql('dim_time',con=engine, if_exists='append',index=False,chunksize=10_000,method='multi')
            new_merchants.to_sql('dim_merchants',con=engine, if_exists='append',index=False,chunksize=10_000,method='multi')

            merchants = pd.read_sql('select merchant_id from dim_merchants', con=engine)
            time = pd.read_sql('select full_time,id from dim_time', con=engine)
            date = pd.read_sql('select full_date,id from dim_date', con=engine)

            print(date.dtypes)


            #df_times.to_sql('dim_time', con=engine, if_exists='append', index=False, chunksize=10_000, method='multi')
            # 🔥 Procesar en chunks para evitar MemoryError
            chunk_size = 50_000  # Reducido para menor consumo de RAM
            id_counter = 1 
            for chunk in np.array_split(df_transactions, len(df_transactions) // chunk_size + 1):

                #chunk['full_date'] = pd.to_datetime(chunk['full_date'])
                #chunk['full_time'] = pd.to_datetime(chunk['full_time'])

                chunk = chunk.merge(date, on="full_date",how="left")
                
                chunk.rename(columns={"id_y": "date_id"}, inplace=True)

                chunk = chunk.merge(time, on="full_time", how="left")

                chunk.rename(columns={"id": "time_id"}, inplace=True)  # id de df_times

                # 3. Ajustar merchant_id 
                chunk.rename(columns={"id_x": "merchant_id"}, inplace=True)
                chunk['id'] = range(id_counter, id_counter + len(chunk))
                id_counter += len(chunk)  # Actualizar contador global

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

        except Exception as e:
            raise FintechException(e,sys)


if __name__ == '__main__':
        
        update = update_db()

        users = pd.read_sql('select * from dim_users', con=engine)
        cards = pd.read_sql('select * from dim_cards', con=engine)

        # Obtener archivos en el bucket
        archives = update.listar_archivos("banks-transaction", 'transactions_part')

        # Obtener archivos ya procesados desde la base de datos
        processed_files = pd.read_sql("SELECT DISTINCT file_name FROM processed_files_log", con=engine)

        # Filtrar archivos nuevos
        new_archives = [file for file in archives if file not in processed_files['file_name'].tolist()]

        if new_archives:
            logging.info(f'Total new archives to process: {len(new_archives)}')
            for i, file in enumerate(new_archives, start=1):
                logging.info(f'Processing {i}/{len(new_archives)}: {file}')
                print(file)

                df = pd.read_csv(f'gs://banks-transaction/{file}')
                update.add_new_data(df)

                # Registrar que el archivo fue procesado
                pd.DataFrame([[file]], columns=['file_name']).to_sql(
                    'processed_files_log', con=engine, if_exists='append', index=False
                )

                client = storage.Client()
                bucket = client.bucket("banks-transaction")
                blob = bucket.blob(file)
                blob.delete()  # Eliminar archivo
                logging.info(f'File {file} has been processed and deleted from the bucket.')

        else:
            logging.info('No new data to add')

