import secrets
from ipaddress import ip_address

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from config import (
    ALLOWED_IPS,
    ALLOWED_IP_NETWORKS,
    API_PASSWORD,
    API_USERNAME,
    API_TOKEN_EXPIRE_MINUTES,
)
from services.auth_service import create_token, verify_token

router = APIRouter(prefix="/auth", tags=["Auth"])
security = HTTPBearer(auto_error=False)


class Credentials(BaseModel):
    username: str
    password: str


def _unauthorized(detail: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        headers={"WWW-Authenticate": "Bearer"},
    )


def _is_ip_allowlisted(request: Request) -> bool:
    forwarded_for = request.headers.get("X-Forwarded-For")

    raw_ip = (
        forwarded_for.split(",")[0].strip()
        if forwarded_for
        else request.client.host if request.client else None
    )

    if not raw_ip:
        return False

    try:
        client_ip = ip_address(raw_ip)
    except ValueError:
        return False

    return client_ip in ALLOWED_IPS or any(
        client_ip in network for network in ALLOWED_IP_NETWORKS
    )


@router.post("/token")
async def generate_token(credentials: Credentials):
    if not (
        secrets.compare_digest(credentials.username, API_USERNAME)
        and secrets.compare_digest(credentials.password, API_PASSWORD)
    ):
        raise _unauthorized("Invalid username or password")

    token = create_token(credentials.username)
    return {
        "access_token": token,
        "token_type": "bearer",
        "expires_in": API_TOKEN_EXPIRE_MINUTES * 60,
    }


def require_token(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
):
    if _is_ip_allowlisted(request):
        return {"sub": "ip-allowlisted"}

    if credentials is None:
        raise _unauthorized("Authorization token is missing")

    return verify_token(credentials.credentials)
