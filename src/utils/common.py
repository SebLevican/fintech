import yaml
from src.exception.exception import FintechException
from src.logging.logger import logging
import os, sys
import numpy as np


def read_yaml_file(file_path: str) -> dict:
    try:
        with open(file_path,'rb') as yaml_file:
            return yaml.safe_load(yaml_file)
    except Exception as e:
        raise FintechException(e,sys)
    
def write_yaml_file(file_path: str, content:object, replace: bool = False) -> None:
    try:
        if replace:
            if os.path.exists(file_path):
                os.remove(file_path)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        with open(file_path,'w') as file:
            yaml.dump(content, file)
    except Exception as e:
        raise FintechException(e,sys)
    



def users_table() -> str:
    query = '''

CREATE TABLE IF NOT EXISTS dim_users (
    id INT PRIMARY KEY,
    gender VARCHAR(10),
    birth_year INT,
    birth_month INT,
    current_age INT,
    retirement_age INT,
    per_capita_income FLOAT,
    yearly_income FLOAT,
    total_debt FLOAT,
    credit_score INT,
    num_credit_cards INT
);
    '''
    return query



def cards_table() -> str:
    query = '''

CREATE TABLE IF NOT EXISTS dim_cards (
    id INT PRIMARY KEY,
    client_id INT,
    card_brand VARCHAR(50),
    card_type VARCHAR(50),
    has_chip BOOLEAN,
    num_cards_issued INT,
    credit_limit FLOAT,
    acct_open_date VARCHAR(7),
    year_pin_last_changed INT,
    card_on_dark_web BOOLEAN,
    FOREIGN KEY (client_id) REFERENCES dim_users(id)
);
    '''
    return query

def merchants_table() -> str:
    query ='''
    
CREATE TABLE IF NOT EXISTS dim_merchants (
    merchant_id PRIMARY KEY,  -- Nueva clave primaria
    merchant_city VARCHAR(255),
    merchant_state VARCHAR(255),
    zip VARCHAR(10),
    mcc INT,
    transaction_id INT UNIQUE
);
    '''
    return query

def date_table() -> str:
    query = '''
    CREATE TABLE IF NOT EXISTS dim_date (
    id SERIAL PRIMARY KEY,
    full_date DATE UNIQUE,
    year INT,
    month INT,
    day INT,
    quarter INT,
    week_of_year INT
);
'''
    return query

def time_table() -> str:
    query = '''
    CREATE TABLE IF NOT EXISTS dim_time (
    id SERIAL PRIMARY KEY,
    full_time TIME UNIQUE,
    hour INT,
    minute INT,
    second INT
);
'''
    return query

def transactions_table() -> str:
    query = '''
CREATE TABLE IF NOT EXISTS fact_transactions (
    id INT PRIMARY KEY,
    client_id INT,
    card_id INT,
    merchant_id INT,
    date_id INT,
    time_id INT,
    amount FLOAT,
    use_chip VARCHAR(20),
    errors VARCHAR(40),
    FOREIGN KEY (client_id) REFERENCES dim_users(id),
    FOREIGN KEY (card_id) REFERENCES dim_cards(id),
    FOREIGN KEY (merchant_id) REFERENCES dim_merchants(merchant_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(id),
    FOREIGN KEY (time_id) REFERENCES dim_time(id)
);
'''
    return query






