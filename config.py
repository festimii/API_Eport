import os

DB_SERVER = os.getenv("DB_SERVER", "192.168.100.10")
DB_NAME = os.getenv("DB_NAME", "wtrgksvf")
DB_USER = os.getenv("DB_USER", "festim.beqiri")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Festimeliza123")

CONNECTION_STRING = (
    f"DRIVER={{SQL Server}};"
    f"SERVER={DB_SERVER};"
    f"DATABASE={DB_NAME};"
    f"UID={DB_USER};"
    f"PWD={DB_PASSWORD};"
)
