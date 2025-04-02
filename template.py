import os
from pathlib import Path
import logging


logging.basicConfig(level=logging.INFO, format='[%(asctime)s]: %(message)s')
project_name='datascience'

list_of_files=[
    ".github/workflows/.gitkeep",
    f'src/__init__.py',
    f'src/components/__init__.py',
    f'src/utils/__init__.py',
    f'src/utils/common.py',
    f'src/config/configuration.py',
    f'src/pipeline/__init__.py',
    f'src/entity/__init__.py',
    f'src/entity/config_entity.py',
    f'src/constants/__init__.py',
    f'src/data_schema/schema.yaml',
    'config/config.yaml',
    'schema.yaml',
    'main.py',
    "Dockerfile",
    'setup.py',
    'params.yaml'
    'research/research.ipynb',
    'templates/index.html'
]


for filepath in list_of_files:
    filepath= Path(filepath)
    filedir,filename=os.path.split(filepath)

    if filedir!="":
        os.makedirs(filedir,exist_ok=True)
        logging.info(f'Creating directory {filedir} for the file : {filename}')

    if (not os.path.exists(filepath)) or (os.path.getsize(filepath) == 0):
        with open(filepath,"w") as f:

            pass
            
            logging.info(f'Creating empty file: {filepath}')

    else:
        logging.info(f'{filename} is already exists')