import psycopg2
from flask import jsonify, Flask
from config import Config
from constants import *

app = Flask(__name__)
app.config.from_object(Config)


@app.route("/api/warnings", methods=["GET"])
def get_warnings():
    def get_info(item: str) -> dict:

        res = {}

        for item in item.split(';')[:-1]:
            key, value = item.split('=')
            res[key] = value

        return res

    try:
        with open("resources/sql/connect.sql") as file:
            tenant_sql = file.read().strip()

        conn = psycopg2.connect(app.config['DATABASE_URI'])

        cur = conn.cursor()
        cur.execute(tenant_sql)

        ans = {}

        for tenantid, name, connectionstring in cur:
            if HOST in connectionstring:
                ans[tenantid] = get_info(connectionstring)

        cur.close()
        conn.close()

        return jsonify(ans)

    except Exception as e:
        return jsonify({"Không thể kết nối tới database": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True)
