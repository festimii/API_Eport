import pyodbc
from config import CONNECTION_STRING

def get_conn():
    return pyodbc.connect(CONNECTION_STRING)
