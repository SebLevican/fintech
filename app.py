from fastapi import FastAPI, Response
from conn import engine
from sqlalchemy import text
import pandas as pd
import json
import subprocess
import os
import sys
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import RedirectResponse
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi import Request
from pydantic import BaseModel
from typing import Dict

# Inicializa la aplicación FastAPI
app = FastAPI()

# Configuración de CORS
origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=['*']
)

# Inicializa Jinja2Templates
templates = Jinja2Templates(directory='./templates')

# Inicialización de pipeline_stats
STATS_FILE = "stats.json"

class DataFrameStats(BaseModel):
    ingestion: int = 0
    validation: int = 0
    transformation: int = 0

class PipelineStats(BaseModel):
    stats: Dict[str, DataFrameStats]

@app.get('/transactions', response_class=HTMLResponse)
def get_transactions(request: Request, redirect: bool = False):
    if redirect:
        return RedirectResponse(url='/transactions',status_code=307)
    query = "SELECT * FROM fact_transactions limit 10"
    with engine.connect() as connection:
        result = connection.execute(text(query))
        rows = result.fetchall()
        columns = result.keys()
        df = pd.DataFrame(rows, columns=columns)
        
        # Convertir el dataframe a HTML
        table_html = df.to_html(classes="table table-striped", index=False)  # Añadir clases para diseño con Bootstrap
        
        # Renderizar la plantilla con los datos
        return templates.TemplateResponse("table.html", {"request": request, "table": table_html})

@app.get("/pipeline_stats")
def get_pipeline_stats():
    if os.path.exists(STATS_FILE):
        with open(STATS_FILE, "r") as file:
            data = json.load(file)
    else:
        data = {}

    return JSONResponse(content=data, media_type="application/json")

@app.get('/update_table')
def get_update():
    try:
        # Ejecuta el script main.py usando subprocess
        result = subprocess.run([sys.executable, 'update_table.py'], capture_output=True, text=True)

        # Si la ejecución fue exitosa, retorna el resultado
        if result.returncode == 0:
            return Response(content=result.stdout, media_type="text/plain")
        else:
            return Response(content=result.stderr, media_type="text/plain", status_code=500)
    except Exception as e:
        return Response(content=str(e), media_type="text/plain", status_code=500)        


@app.get('/',tags=['authentication'])
async def index():
    return RedirectResponse(url='/docs')

@app.get('/execute_main')
def execute_main():
    try:
        # Ejecuta el script main.py usando subprocess
        result = subprocess.run([sys.executable, 'main.py'], capture_output=True, text=True)

        # Si la ejecución fue exitosa, retorna el resultado
        if result.returncode == 0:
            return Response(content=result.stdout, media_type="text/plain")
        else:
            return Response(content=result.stderr, media_type="text/plain", status_code=500)
    except Exception as e:
        return Response(content=str(e), media_type="text/plain", status_code=500)
