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


try:
    resp = requests.get("http://127.0.0.1:5000/api/warnings").json()

    out = defaultdict(list)

    for tenant_id, conn_str in resp.items():
        out[tuple(conn_str.items())].append(tenant_id)

    with open("../resources/sql/querry.sql") as sql_file:
        sending_tax = sql_file.read()

    result = []
    count = 0

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
                df = pd.read_sql_query(sending_tax, conn)
                result.append(df)
        except Exception as e:
            count += 1

        time.sleep(3)

    try:
        engine = create_engine(Config.DATABASE_URI)

        with engine.connect() as conn:
            df = pd.read_sql_query(sending_tax, conn)
            result.append(df)
    except Exception as e:
        count += 1

    print(count)

    ans = pd.concat(result, ignore_index=True)

    if len(ans) != 0:
        os.makedirs(Config.ERROR_INVOICE_FOLDER, exist_ok=True)
        file_path = os.path.join(Config.ERROR_INVOICE_FOLDER, Config.ERROR_INVOICE_FILE)

        data = list(ans.to_dict(orient="index").values())
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    # if len(df) == 0:
    #     engine = create_engine(Config.DATABASE_URI)
    #     df = pd.read_sql_query(sending_tax, con=engine)
    #     print(df)

except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL", error)
