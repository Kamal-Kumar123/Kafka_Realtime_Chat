#
#  main.py
#  fastapi_kafka
#
#  Created by Xavier Cañadas on 15/4/2025
#  Copyright (c) 2025. All rights reserved.

from contextlib import asynccontextmanager
import re
import secrets
import string
from typing import Annotated
from sqlmodel import Session, select, inspect

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.security import OAuth2PasswordRequestForm
from passlib.context import CryptContext
from pydantic import BaseModel, ConfigDict, ValidationError
import logging

from .jwt_auth import (
    ACCESS_TOKEN_EXPIRATION,
    oauth2_scheme,
    create_access_token,
    get_username_from_token,
    jwt_keys_ready,
)
from .database import SessionDep, engine
from .models import User

# set up the loggin in docker
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class Token(BaseModel):
    access_token: str
    token_type: str
    username: str
    email: str


class UserCreate(BaseModel):
    username: str | None = None
    first_name: str
    last_name: str
    email: str
    password: str


class GoogleAuthRequest(BaseModel):
    email: str
    name: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    google_sub: str | None = None


class UserPublic(BaseModel):
    username: str
    first_name: str
    last_name: str
    email: str
    disabled: bool

    model_config = ConfigDict(from_attributes=True)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup
    #create_db_and_tables()
    yield
    # shutdown

app = FastAPI(lifespan=lifespan)

@app.get("/db-check")
def check_db():
    inspector = inspect(engine)
    tables = inspector.get_table_names(schema="public")
    return {"tables": tables}



def verify_password(plain_password: str, hashed_password: str):
    if not hashed_password:
        return False
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str):
    return pwd_context.hash(password)


def get_user(session: Session, username: str) -> User | None:
    user = session.get(User, username)
    return user


def get_user_by_email(session: Session, email: str) -> User | None:
    statement = select(User).where(User.email == email.lower())
    return session.exec(statement).first()


def is_valid_email(email: str) -> bool:
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email))


def build_username_from_email(email: str, session: Session) -> str:
    local_part = email.split("@", 1)[0].lower()
    base_username = "".join(
        char for char in local_part if char in string.ascii_lowercase + string.digits + "._-"
    ).strip("._-") or "user"

    username = base_username[:50]
    suffix = 1
    while get_user(session, username):
        suffix_text = f".{suffix}"
        username = f"{base_username[:50 - len(suffix_text)]}{suffix_text}"
        suffix += 1

    return username


def create_user_record(user_request: UserCreate, session: Session) -> User:
    email = user_request.email.lower().strip()
    username = (user_request.username or build_username_from_email(email, session)).lower().strip()

    if not is_valid_email(email):
        raise HTTPException(status_code=400, detail="Enter a valid email address")

    if len(user_request.password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters")

    if get_user(session, username):
        raise HTTPException(status_code=400, detail="Username already registered")

    if get_user_by_email(session, email):
        raise HTTPException(status_code=400, detail="Email already registered")

    db_user = User(
        username=username,
        first_name=user_request.first_name.strip(),
        last_name=user_request.last_name.strip(),
        email=email,
        password_hash=get_password_hash(user_request.password)
    )
    session.add(db_user)
    session.commit()
    session.refresh(db_user)
    return db_user


def create_access_token_for_user(user: User) -> Token:
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=ACCESS_TOKEN_EXPIRATION
    )
    logger.info(f"Access token created for user: {user.username}")
    return Token(
        access_token=access_token,
        token_type="bearer",
        username=user.username,
        email=user.email,
    )


def authenticate_user(session: Session, identifier: str, password: str):
    normalized_identifier = identifier.lower().strip()
    user = get_user(session, normalized_identifier)
    if not user and is_valid_email(normalized_identifier):
        user = get_user_by_email(session, normalized_identifier)

    if not user:
        return False
    if not verify_password(password, user.password_hash):
        return False
    return user



async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)], session: SessionDep):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    username = get_username_from_token(token)

    user = get_user(session, username=username)

    if user is None:
        raise credentials_exception

    return user


async def get_current_active_user(current_user: Annotated[User, Depends(get_current_user)]):
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user


@app.post("/token")
async def login(form_data: Annotated[OAuth2PasswordRequestForm, Depends()], session: SessionDep) -> Token:
    user = authenticate_user(session, form_data.username, form_data.password)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return create_access_token_for_user(user)


@app.post("/users", response_model=UserPublic)
async def create_user(request: Request, session: SessionDep):
    """
    Register a user. Accepts JSON, form data, or query parameters to keep older
    local HTTP-client requests working while the web UI moves to JSON.
    """
    data = dict(request.query_params)
    if not data:
        content_type = request.headers.get("content-type", "")
        if "application/json" in content_type:
            data = await request.json()
        elif "form" in content_type:
            form = await request.form()
            data = dict(form)

    try:
        user_request = UserCreate(**data)
    except ValidationError as error:
        raise HTTPException(status_code=400, detail=error.errors())

    return create_user_record(user_request, session)


@app.post("/auth/google", response_model=Token)
def google_login(google_request: GoogleAuthRequest, session: SessionDep):
    if not jwt_keys_ready():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "JWT keys missing on login_server. On Render, set JWT_PRIVATE_KEY and "
                "JWT_PUBLIC_KEY (paste PEM contents from auxiliar/keys)."
            ),
        )

    try:
        email = google_request.email.lower().strip()
        if not is_valid_email(email):
            raise HTTPException(
                status_code=400, detail="Google account did not return a valid email"
            )

        user = get_user_by_email(session, email)
        if not user:
            first_name = (google_request.first_name or "").strip()
            last_name = (google_request.last_name or "").strip()

            if not first_name and google_request.name:
                name_parts = google_request.name.strip().split(" ", 1)
                first_name = name_parts[0]
                last_name = name_parts[1] if len(name_parts) > 1 else ""

            user = User(
                username=build_username_from_email(email, session),
                first_name=first_name or "Google",
                last_name=last_name or "User",
                email=email,
                password_hash=get_password_hash(secrets.token_urlsafe(32)),
            )
            session.add(user)
            session.commit()
            session.refresh(user)

        return create_access_token_for_user(user)
    except HTTPException:
        raise
    except Exception as error:
        logger.exception("Google auth failed: %s", error)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Google sign-in failed on server: {error}",
        ) from error

@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/health")
def health(session: SessionDep):
    """Use this on Render to verify DB + JWT keys before Google login."""
    db_ok = False
    try:
        session.exec(select(User).limit(1)).first()
        db_ok = True
    except Exception as error:
        logger.error("Health DB check failed: %s", error)

    return {
        "status": "ok" if db_ok and jwt_keys_ready() else "degraded",
        "database": db_ok,
        "jwt_keys": jwt_keys_ready(),
    }


@app.get("/users/me", response_model=UserPublic)
async def read_user_me(current_user: Annotated[User, Depends(get_current_user)]):
    return current_user


