import os

DB_SERVER = os.getenv("DB_SERVER", "192.168.100.10")
DB_NAME = os.getenv("DB_NAME", "wtrgksvf")
DB_USER = os.getenv("DB_USER", "festim.beqiri")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Festimeliza123")

API_USERNAME = os.getenv("API_USERNAME", "admin")
API_PASSWORD = os.getenv("API_PASSWORD", "API_TEST")
API_SECRET_KEY = os.getenv("API_SECRET_KEY", "LKk100mbviva5589")
API_TOKEN_EXPIRE_MINUTES = int(os.getenv("API_TOKEN_EXPIRE_MINUTES", "60"))

CONNECTION_STRING = (
    f"DRIVER={{SQL Server}};"
    f"SERVER={DB_SERVER};"
    f"DATABASE={DB_NAME};"
    f"UID={DB_USER};"
    f"PWD={DB_PASSWORD};"
)
