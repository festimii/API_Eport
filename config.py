import os
from typing import Final
from ipaddress import ip_address, ip_network
from dotenv import load_dotenv

load_dotenv()


# =========================
# Exceptions
# =========================

class MissingEnvironmentVariable(RuntimeError):
    """Raised when a required environment variable is not provided."""


class InvalidConfiguration(RuntimeError):
    """Raised when configuration values are invalid."""


# =========================
# Helpers
# =========================

def _require_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value or not value.strip():
        raise MissingEnvironmentVariable(
            f"Environment variable {var_name} must be set for the API to start securely."
        )
    return value.strip()


def _parse_ip(value: str):
    try:
        return ip_address(value)
    except ValueError as exc:
        raise InvalidConfiguration(f"Invalid IP address in ALLOWED_IPS: {value}") from exc


def _parse_network(value: str):
    try:
        return ip_network(value, strict=False)
    except ValueError as exc:
        raise InvalidConfiguration(f"Invalid network in ALLOWED_IP_PREFIXES: {value}") from exc


# =========================
# Database
# =========================

DB_SERVER: Final[str] = _require_env("DB_SERVER")
DB_NAME: Final[str] = _require_env("DB_NAME")
DB_USER: Final[str] = _require_env("DB_USER")
DB_PASSWORD: Final[str] = _require_env("DB_PASSWORD")


# =========================
# API Security
# =========================

API_USERNAME: Final[str] = _require_env("API_USERNAME")
API_PASSWORD: Final[str] = _require_env("API_PASSWORD")
API_SECRET_KEY: Final[str] = _require_env("API_SECRET_KEY")
API_TOKEN_EXPIRE_MINUTES: Final[int] = int(os.getenv("API_TOKEN_EXPIRE_MINUTES", "60"))


# =========================
# IP Allowlist (STRICT)
# =========================

# Explicit IPs only
ALLOWED_IPS: Final[set] = {
    _parse_ip("34.165.250.146"),
    _parse_ip("46.99.210.54"),
    _parse_ip("34.165.194.156"),
    _parse_ip("31.154.21.146"),
    _parse_ip("62.90.169.6"),
    _parse_ip("127.0.0.1"),
    _parse_ip("::1"),
}

# Proper CIDR networks (NOT string prefixes)
ALLOWED_IP_NETWORKS: Final[tuple] = (
    _parse_network("192.168.0.0/16"),
)


# =========================
# Database Connection
# =========================

CONNECTION_STRING: Final[str] = (
    f"DRIVER={{SQL Server}};"
    f"SERVER={DB_SERVER};"
    f"DATABASE={DB_NAME};"
    f"UID={DB_USER};"
    f"PWD={DB_PASSWORD};"
    f"Encrypt=no;"
    f"TrustServerCertificate=yes;"
)
