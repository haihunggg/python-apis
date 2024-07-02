import json

import requests
import psycopg2
import pandas as pd
import time
import os
from collections import defaultdict
from urllib.parse import quote_plus
from config import Config
from sqlalchemy import create_engine

from datetime import datetime as dt
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler("app.log")
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s")
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

logger.addHandler(file_handler)
logger.addHandler(console_handler)


def format_file_name():
    now = str(dt.now())
    dot_index = now.rfind('.')
    return now[:dot_index].replace(' ', '_').replace('-', '').replace(':', '-')


try:
    logger.info("Starting the application")
    resp = requests.get("http://127.0.0.1:5000/api/warnings").json()
    # resp = {'3a096fa2-9489-4c0e-7cb1-1b8792822520': {'Database': 'Master_20230214', 'Host': '10.10.12.17', 'Password': 'Minvoice@123', 'Port': '5432', 'User ID': 'minvoice'}}
    out = defaultdict(list)

    for tenant_id, conn_str in resp.items():
        out[tuple(conn_str.items())].append(tenant_id)

    with open("resources/sql/querry.sql") as sql_file:
        sending_tax = sql_file.read()

    result = []
    count = 0
    db_errors = []

    for db_info, tenant_ids in out.items():
        db = dict(db_info)

        database = db["Database"]
        user = db["User ID"]
        password = db["Password"]
        host = db["Host"]
        port = db["Port"]
        encoded_password = quote_plus(password)
        DATABASE_URI = f'postgresql://{user}:{encoded_password}@{host}:{port}/{database}'

        try:
            
            engine = create_engine(DATABASE_URI)
            with engine.connect() as conn:
                for ten_id in tenant_ids:
                    querry_sending_tax = sending_tax.replace('?', ten_id)
                    df = pd.read_sql_query(querry_sending_tax, conn)
                    result.append(df)
        except Exception as e:
            db_errors.append(DATABASE_URI)
            count += 1

        time.sleep(3)

    try:
        engine = create_engine(Config.DATABASE_URI)

        with engine.connect() as conn:
            df = pd.read_sql_query(sending_tax, conn)
            result.append(df)
    except Exception as e:
        db_errors.append(Config.DATABASE_URI)
        count += 1

    errors = '\n'.join(db_errors)
    logger.info(f"error db number: {count}\n{errors}")

    ans = pd.concat(result, ignore_index=True)

    if len(ans) != 0:
        os.makedirs(Config.ERROR_INVOICE_FOLDER, exist_ok=True)
        file_name = f"{format_file_name()}.json"
        file_path = os.path.join(Config.ERROR_INVOICE_FOLDER, file_name)

        data = list(ans.to_dict(orient="index").values())
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    # if len(df) == 0:
    #     engine = create_engine(Config.DATABASE_URI)
    #     df = pd.read_sql_query(sending_tax, con=engine)
    #     print(df)
    logger.info("done")

except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL", error)
