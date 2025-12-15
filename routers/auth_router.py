import secrets
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from config import API_PASSWORD, API_USERNAME, API_TOKEN_EXPIRE_MINUTES
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
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
):
    if credentials is None:
        raise _unauthorized("Authorization token is missing")

    return verify_token(credentials.credentials)
