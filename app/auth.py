"""
auth.py — Authentication layer.

Responsibilities:
    - Password hashing (bcrypt via passlib)
    - Refresh token hashing (SHA-256 — sufficient for random strings)
    - Access token creation and verification (JWT via PyJWT)
    - FastAPI dependencies: get_current_user (HTTP), get_ws_user (WebSocket)

Nothing in this file touches the database. DB operations for storing and
revoking refresh tokens live in db.py to keep the boundary clean.
"""

import hashlib
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

import jwt
from fastapi import Depends, HTTPException, Query, WebSocket, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pwdlib import PasswordHash

from app.config import ACCESS_TOKEN_EXPIRE_MINUTES, JWT_SECRET

# Password hashing
password_hash = PasswordHash.recommended()


def hash_password(plain: str) -> str:
    return password_hash.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    return password_hash.verify(plain, hashed)


# Refresh token hashing

# Refresh tokens are cryptographically random strings (not user-chosen), so
# SHA-256 is sufficient to protect them at rest. bcrypt's slow hashing is
# designed to resist brute-force attacks on dictionary-guessable inputs —
# overkill when the input is 64 bytes of random data.


def hash_refresh_token(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


def generate_refresh_token() -> str:
    """64 URL-safe random bytes → 86-character string."""
    return secrets.token_urlsafe(64)


# JWT — access tokens only

# Refresh tokens are NOT JWTs. They're opaque random strings stored in the DB.
# Only access tokens are JWTs — they need to be verified without a DB lookup.


def create_access_token(user_id: str, name: str) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub": user_id,  # subject — standard JWT claim
        "name": name,
        "iat": now,  # issued at
        "exp": now + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
        "type": "access",  # guard against using a refresh JWT as access
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")


def decode_access_token(token: str) -> dict:
    """
    Decode and validate an access token. Raises HTTPException on any failure
    so callers don't need to handle jwt exceptions directly.
    """
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        if payload.get("type") != "access":
            raise jwt.InvalidTokenError("Not an access token")
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Access token has expired",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid access token",
            headers={"WWW-Authenticate": "Bearer"},
        )


# FastAPI dependencies

_bearer = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer),
) -> dict:
    """
    Dependency for protected HTTP routes.

    Usage:
        @app.post("/posts")
        async def create_post(current_user: dict = Depends(get_current_user)):
            user_id = current_user["sub"]

    Returns the decoded JWT payload. No DB hit — the token is self-contained.
    """
    return decode_access_token(credentials.credentials)


async def get_ws_user(
    websocket: WebSocket,
    token: Optional[str] = Query(default=None),
) -> dict:
    """
    Dependency for authenticated WebSocket endpoints.

    Browser WebSocket API does not support custom headers, so the access token
    is passed as a query parameter:
        ws://localhost:8000/ws/feed?token=<access_token>

    Close codes used:
        4001 — missing token
        4003 — invalid or expired token
    """
    if not token:
        await websocket.close(code=4001)
        raise HTTPException(status_code=401, detail="Missing token")
    try:
        return decode_access_token(token)
    except HTTPException:
        await websocket.close(code=4003)
        raise
