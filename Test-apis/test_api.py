import datetime
import pytz
import requests
import psycopg2
from collections import defaultdict
from config import Config
from datetime import timedelta, timezone
from constants import CREATIONTIME


def execute_sql(conn, sql_query):
    cur = conn.cursor()
    cur.execute(sql_query)
    return cur


tz_hcm = pytz.timezone('Asia/Ho_Chi_Minh')
conn = cur = None

try:
    resp = requests.get("http://127.0.0.1:5000/api/warnings").json()

    out = defaultdict(list)

    for tenant_id, conn_str in resp.items():
        out[tuple(conn_str.items())].append(tenant_id)

    with open("../resources/sql/querry.sql") as sql_file:
        sending_tax = sql_file.read()

    for db_info, tenant_ids in out.items():
        db = dict(db_info)

        conn = psycopg2.connect(
            database=db["Database"],
            user=db["User ID"],
            password=db["Password"],
            host=db["Host"],
            port=db["Port"]
        )

        cur = execute_sql(conn, sending_tax)

        if not cur.fetchall():
            conn = psycopg2.connect(Config.DATABASE_URI)

            cur = execute_sql(conn, sending_tax)
            now = datetime.datetime.now()
            start_time = now - timedelta(hours=CREATIONTIME)

            for row in cur:
                dt: datetime.datetime = row[1]
                ans = datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.microsecond, tzinfo=None)
                if start_time <= ans <= now:
                    print(row[1])

        conn.close()
        break

except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()
