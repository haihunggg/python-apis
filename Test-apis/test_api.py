import datetime
import pytz
import requests
import psycopg2
import pandas as pd
from collections import defaultdict
from config import Config
from urllib.parse import quote_plus
from sqlalchemy import create_engine


try:
    resp = requests.get("http://127.0.0.1:5000/api/warnings").json()

    out = defaultdict(list)

    for tenant_id, conn_str in resp.items():
        out[tuple(conn_str.items())].append(tenant_id)

    with open("../resources/sql/querry.sql") as sql_file:
        sending_tax = sql_file.read()

    for db_info, tenant_ids in out.items():
        db = dict(db_info)

        database = db["Database"],
        user = db["User ID"],
        password = db["Password"],
        host = db["Host"],
        port = db["Port"]

        DATABASE_URI = f'postgresql://{user}:{password}@{host}:{port}/{database}'

        engine = create_engine(Config.DATABASE_URI)

        df = pd.read_sql_query(sending_tax, con=engine)

        if len(df) == 0:
            engine = create_engine(Config.DATABASE_URI)
            df = pd.read_sql_query(sending_tax, con=engine)
            print(df)

except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL", error)
