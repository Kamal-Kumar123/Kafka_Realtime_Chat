#
#  jwt_auth.py
#  fastapi_kafka
#
#  Created by Xavier Cañadas on 15/4/2025
#  Copyright (c) 2025. All rights reserved.

import os

import jwt
from fastapi import HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jwt.exceptions import InvalidTokenError

ALGORITHM = os.getenv("ALGORITHM", "RS256")


def _load_pem(path_env: str, inline_env: str) -> bytes | None:
    inline = os.getenv(inline_env)
    if inline:
        return inline.replace("\\n", "\n").encode("utf-8")

    path = os.getenv(path_env)
    if path and os.path.exists(path):
        with open(path, "rb") as key_file:
            return key_file.read()
    return None


PUBLIC_KEY = _load_pem("PUBLIC_KEY_PATH", "JWT_PUBLIC_KEY")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


def get_username_from_token(token: str):
    if not PUBLIC_KEY:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="JWT public key not configured. Set JWT_PUBLIC_KEY on Render.",
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
