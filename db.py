import pyodbc
from config import CONNECTION_STRING, CONNECTION_STRING2


def get_conn():
    return pyodbc.connect(CONNECTION_STRING)


def get_conn2():
    return pyodbc.connect(CONNECTION_STRING2)
