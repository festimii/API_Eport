import os
from typing import Final


class MissingEnvironmentVariable(RuntimeError):
    """Raised when a required environment variable is not provided."""


def _require_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if value is None or value.strip() == "":
        raise MissingEnvironmentVariable(
            f"Environment variable {var_name} must be set for the API to start securely."
        )
    return value


DB_SERVER: Final[str] = _require_env("DB_SERVER")
DB_NAME: Final[str] = _require_env("DB_NAME")
DB_USER: Final[str] = _require_env("DB_USER")
DB_PASSWORD: Final[str] = _require_env("DB_PASSWORD")

API_USERNAME: Final[str] = _require_env("API_USERNAME")
API_PASSWORD: Final[str] = _require_env("API_PASSWORD")
API_SECRET_KEY: Final[str] = _require_env("API_SECRET_KEY")
API_TOKEN_EXPIRE_MINUTES: Final[int] = int(os.getenv("API_TOKEN_EXPIRE_MINUTES", "60"))

ALLOWED_IPS: Final[set[str]] = {
    "34.165.250.146",
    "34.165.194.156",
    "31.154.21.146",
    "62.90.169.6",
    "localhost",
    "127.0.0.1",
    "::1",
}

ALLOWED_IP_PREFIXES: Final[tuple[str, ...]] = ("192.168.",)

CONNECTION_STRING: Final[str] = (
    f"DRIVER={{SQL Server}};"
    f"SERVER={DB_SERVER};"
    f"DATABASE={DB_NAME};"
    f"UID={DB_USER};"
    f"PWD={DB_PASSWORD};"
)
