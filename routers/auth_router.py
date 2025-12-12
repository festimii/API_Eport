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


@router.post("/token")
async def generate_token(credentials: Credentials):
    if credentials.username != API_USERNAME or credentials.password != API_PASSWORD:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password",
        )

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
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization token is missing",
        )
    verify_token(credentials.credentials)
