#
#  jwt_auth.py
#  fastapi_kafka
#
#  Created by Xavier Cañadas on 15/4/2025
#  Copyright (c) 2025. All rights reserved.

import os
from datetime import datetime, timedelta, timezone

import jwt
from fastapi import HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jwt.exceptions import InvalidTokenError
from pydantic import BaseModel

ALGORITHM = os.getenv("ALGORITHM", "RS256")
ACCESS_TOKEN_EXPIRATION = timedelta(days=30)


def _load_pem(path_env: str, inline_env: str) -> bytes | None:
    """Load PEM from file path or Render env var (use \\n for newlines in dashboard)."""
    inline = os.getenv(inline_env)
    if inline:
        return inline.replace("\\n", "\n").encode("utf-8")

    path = os.getenv(path_env)
    if path and os.path.exists(path):
        with open(path, "rb") as key_file:
            return key_file.read()
    return None


PUBLIC_KEY = _load_pem("PUBLIC_KEY_PATH", "JWT_PUBLIC_KEY")
PRIVATE_KEY = _load_pem("PRIVATE_KEY_PATH", "JWT_PRIVATE_KEY")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


class TokenData(BaseModel):
    username: str | None = None


def jwt_keys_ready() -> bool:
    return bool(PRIVATE_KEY and PUBLIC_KEY)


def create_access_token(data: dict, expires_delta: timedelta | None = None):
    if not PRIVATE_KEY:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "JWT private key not configured. On Render, set JWT_PRIVATE_KEY "
                "(full PEM) or mount private_key.pem and set PRIVATE_KEY_PATH."
            ),
        )

    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=15)

    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, PRIVATE_KEY, algorithm=ALGORITHM)


def get_username_from_token(token: str):
    if not PUBLIC_KEY:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="JWT public key not configured.",
        )

    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(token, PUBLIC_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        return username
    except InvalidTokenError:
        raise credentials_exception
