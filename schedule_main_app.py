import time
import subprocess


def job():
    path = r"C:\Users\Admin\PycharmProjects\python-apis\main_app.py"
    subprocess.run(["python", path])


while True:
    job()
    time.sleep(60)
