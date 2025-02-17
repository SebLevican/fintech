FROM python:3.10-slim-buster

# Establecer el directorio de trabajo
WORKDIR /app

# Copiar los archivos del servicio al contenedor
COPY . /app

# Instalar dependencias necesarias
RUN apt update -y && apt install awscli -y
RUN pip install -r requirements.txt
RUN pip install fastapi google-cloud-storage uvicorn

# Comando para ejecutar el servicio
CMD ["uvicorn", "gcs_app:app", "--host", "0.0.0.0", "--port", "5001"]