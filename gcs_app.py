from fastapi import FastAPI
from google.cloud import storage

import json

app = FastAPI()

# Cargar las variables de entorno desde el archivo .env


# Asegúrate de que la cuenta de servicio de Google Cloud tenga acceso a los buckets
client = storage.Client()

@app.get("/mcc_codes", response_model=dict)
async def get_mcc_codes():
    try:
        # Acceder al archivo mcc_codes.json desde el bucket de Google Cloud
        bucket_name = "banks-transaction"  # Cambia esto por tu bucket
        bucket = client.get_bucket(bucket_name)
        
        # Obtener el blob para mcc_codes.json
        mcc_blob = bucket.blob("mcc_codes.json")
        
        # Descargar el archivo JSON y convertirlo en un objeto Python
        mcc_data = json.loads(mcc_blob.download_as_text())
        
        return mcc_data
    except Exception as e:
        return {"error": f"Error al obtener mcc_codes: {str(e)}"}

@app.get("/train_fraud_labels", response_model=dict)
async def get_train_fraud_labels():
    try:
        # Acceder al archivo train_fraud_labels.json desde el bucket de Google Cloud
        bucket_name = "banks-transaction"  # Cambia esto por tu bucket
        bucket = client.get_bucket(bucket_name)
        
        # Obtener el blob para train_fraud_labels.json
        fraud_labels_blob = bucket.blob("train_fraud_labels.json")
        
        # Descargar el archivo JSON y convertirlo en un objeto Python
        fraud_labels_data = json.loads(fraud_labels_blob.download_as_text())
        
        return fraud_labels_data
    except Exception as e:
        return {"error": f"Error al obtener train_fraud_labels: {str(e)}"}
