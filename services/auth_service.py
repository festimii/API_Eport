import base64
import hmac
import json
import time
from hashlib import sha256
from typing import Any

from fastapi import HTTPException, status

from config import API_SECRET_KEY, API_TOKEN_EXPIRE_MINUTES


def _unauthorized(detail: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        headers={"WWW-Authenticate": "Bearer"},
    )


def _sign(message: bytes) -> str:
    return hmac.new(API_SECRET_KEY.encode(), message, sha256).hexdigest()


def _decode_payload(encoded_payload: str) -> dict[str, Any]:
    padding = "=" * (-len(encoded_payload) % 4)
    payload_bytes = base64.urlsafe_b64decode(encoded_payload + padding)
    return json.loads(payload_bytes.decode())


def create_token(subject: str) -> str:
    expires_at = int(time.time()) + (API_TOKEN_EXPIRE_MINUTES * 60)
    payload = {"sub": subject, "exp": expires_at}
    payload_bytes = json.dumps(payload, separators=(',', ':')).encode()
    encoded_payload = base64.urlsafe_b64encode(payload_bytes).decode().rstrip('=')
    signature = _sign(encoded_payload.encode())
    return f"{encoded_payload}.{signature}"


def verify_token(token: str) -> dict[str, Any]:
    try:
        encoded_payload, signature = token.split(".")
    except ValueError:
        raise _unauthorized("Invalid token format")

    expected_signature = _sign(encoded_payload.encode())
    if not hmac.compare_digest(expected_signature, signature):
        raise _unauthorized("Invalid token signature")

    payload = _decode_payload(encoded_payload)
    expires_at = payload.get("exp")
    if not isinstance(expires_at, int) or expires_at < int(time.time()):
        raise _unauthorized("Token has expired")

    return payload
