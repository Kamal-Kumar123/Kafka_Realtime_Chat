#
#  main.py
#  fastapi_kafka
#
#  Created by GitHub Copilot on 16/4/2025
#  Copyright (c) 2025. All rights reserved.

import json
import time
import uuid
from typing import Annotated, Any, Optional, Dict
import os

import requests
try:
    from .service_warmup import (
        CHANNEL_MANAGER_URL,
        LOGIN_SERVER_URL,
        WARMUP_WEBSOCKET_URL,
        _login_server_url,
        start_background_warmup,
        warmup_status_response,
        wake_service,
    )
except ImportError:
    from service_warmup import (
        CHANNEL_MANAGER_URL,
        LOGIN_SERVER_URL,
        WARMUP_WEBSOCKET_URL,
        _login_server_url,
        start_background_warmup,
        warmup_status_response,
        wake_service,
    )
from fastapi import FastAPI, Request, Form, Cookie, Depends, HTTPException, status
from fastapi.responses import HTMLResponse, RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from authlib.integrations.starlette_client import OAuth, OAuthError
from pydantic import BaseModel
from starlette.middleware.sessions import SessionMiddleware

# Configuration (LOGIN_SERVER_URL also defined in service_warmup)
WEBSOCKET_CLIENT_URL = os.getenv(
    "WEBSOCKET_CLIENT_URL", "ws://localhost:5001/ws"
)  # The URL clients use to connect
SESSION_SECRET_KEY = os.getenv("SESSION_SECRET_KEY", "change-me-in-production")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
# Must match an authorized redirect URI in Google Cloud Console exactly.
GOOGLE_REDIRECT_URI = os.getenv(
    "GOOGLE_REDIRECT_URI", "http://localhost:5004/auth/google/callback"
)
APP_CANONICAL_HOST = os.getenv("APP_CANONICAL_HOST", "localhost")

# Initialize FastAPI
app = FastAPI()


class CanonicalHostMiddleware(BaseHTTPMiddleware):
    """Keep OAuth on one host — localhost and 127.0.0.1 use separate cookies."""

    async def dispatch(self, request: Request, call_next):
        if request.url.hostname == "127.0.0.1":
            canonical = str(request.url.replace(hostname=APP_CANONICAL_HOST))
            return RedirectResponse(canonical, status_code=307)
        return await call_next(request)


# Session must wrap the app so OAuth state survives the Google redirect round-trip.
app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET_KEY,
    session_cookie="oauth_session",
    same_site="lax",
    https_only=False,
    max_age=3600,
)
app.add_middleware(CanonicalHostMiddleware)

# Set up templates directory
templates = Jinja2Templates(directory="app/templates")

# Set up static files directory
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Session storage (in production this would be Redis or similar)
active_sessions: Dict[str, Dict] = {}

oauth = OAuth()
if GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET:
    oauth.register(
        name="google",
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
        client_kwargs={"scope": "openid email profile"},
        redirect_uri=GOOGLE_REDIRECT_URI,
    )


def clear_stale_oauth_session(request: Request) -> None:
    """Remove leftover OAuth state from a previous failed sign-in attempt."""
    for key in list(request.session.keys()):
        if key.startswith("_state_") or key.startswith("_google"):
            del request.session[key]


class LoginForm(BaseModel):
    email: str
    password: str


def render_login(request: Request, error_message: str = "", success_message: str = ""):
    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "error_message": error_message,
            "success_message": success_message,
            "google_enabled": bool(GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET),
        },
    )


def create_session_response(username: str, token: str, redirect_url: str = "/channels"):
    session_id = str(uuid.uuid4())
    active_sessions[session_id] = {
        "username": username,
        "token": token,
    }

    response = RedirectResponse(url=redirect_url, status_code=303)
    response.set_cookie(key="session_id", value=session_id, httponly=True, samesite="lax")
    return response


def _wake_login_server() -> None:
    wake_service("login_server", _login_server_url("/health"))


def _request_login_server(method: str, path: str, **kwargs: Any) -> requests.Response:
    """
    Call login_server with retries for transient 502/503 (cold start on Render).
    """
    url = _login_server_url(path)
    retries = int(os.getenv("LOGIN_SERVER_RETRIES", "4"))
    wait_seconds = float(os.getenv("LOGIN_SERVER_RETRY_SECONDS", "3"))
    timeout = int(os.getenv("LOGIN_SERVER_TIMEOUT", "30"))
    kwargs.setdefault("timeout", timeout)

    last_response: requests.Response | None = None
    last_error: requests.RequestException | None = None

    for attempt in range(retries):
        try:
            response = requests.request(method, url, **kwargs)
            last_response = response
            if response.status_code not in (502, 503, 504):
                return response
        except requests.RequestException as error:
            last_error = error

        if attempt < retries - 1:
            time.sleep(wait_seconds * (attempt + 1))

    if last_response is not None:
        return last_response
    assert last_error is not None
    raise last_error


def get_error_message(response: requests.Response, fallback: str) -> str:
    try:
        detail = response.json().get("detail")
        if isinstance(detail, str):
            return detail
        if isinstance(detail, list) and detail:
            return detail[0].get("msg", fallback)
    except Exception:
        pass
    if response.status_code:
        return f"{fallback} (server returned {response.status_code})"
    return fallback


def get_current_user(session_id: Annotated[Optional[str], Cookie()] = None):
    """Dependency to check if user is logged in"""
    if not session_id or session_id not in active_sessions:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
        )
    return active_sessions[session_id]


@app.get("/health")
def health():
    """Fast ping for UptimeRobot — does not wake other services."""
    return {"status": "ok", "service": "web_client"}


@app.get("/api/status")
def api_status():
    """
    Debug Render config: open https://YOUR-web-client.onrender.com/api/status
    """
    login_health_url = _login_server_url("/health")
    result: dict[str, Any] = {
        "web_client": "ok",
        "login_server_url_configured": LOGIN_SERVER_URL,
        "login_server_health_url": login_health_url,
        "channel_manager_url": CHANNEL_MANAGER_URL or "(not set)",
        "warmup_websocket_url": WARMUP_WEBSOCKET_URL or "(not set)",
        "login_server": None,
        "hints": [],
    }

    if "login_server" in LOGIN_SERVER_URL and "onrender.com" not in LOGIN_SERVER_URL:
        result["hints"].append(
            "LOGIN_SERVER_URL still looks like Docker default (http://login_server). "
            "Set it to your public Render URL, e.g. https://realtimechat-login-server.onrender.com"
        )

    try:
        response = requests.get(login_health_url, timeout=20)
        result["login_server"] = {
            "http_status": response.status_code,
            "body": response.json() if response.headers.get("content-type", "").startswith("application/json") else response.text[:500],
        }
        if response.status_code == 404:
            result["hints"].append(
                "login_server returned 404 — wrong URL or old deploy without /health. "
                "Copy the exact URL from Render dashboard → login_server service."
            )
        elif response.status_code in (502, 503, 504):
            result["hints"].append(
                "login_server is sleeping or crashed (free tier). Wait 60s, refresh this page, or use UptimeRobot."
            )
        elif response.status_code == 200:
            body = result["login_server"].get("body") or {}
            if isinstance(body, dict) and not body.get("jwt_keys"):
                result["hints"].append(
                    "Set JWT_PRIVATE_KEY and JWT_PUBLIC_KEY on login_server in Render (paste full PEM files)."
                )
            if isinstance(body, dict) and not body.get("database"):
                result["hints"].append("Set DATABASE_URL on login_server to your Render Postgres internal URL.")
    except requests.RequestException as error:
        result["login_server"] = {"error": str(error)}
        result["hints"].append("Cannot reach login_server — check LOGIN_SERVER_URL and that the service is Live on Render.")

    return result


@app.get("/api/warmup")
def api_warmup():
    """
    Wake all backend microservices (login, channels, websocket).
    Called automatically from the login page; safe to open in a browser.
    """
    return warmup_status_response()


@app.get("/", response_class=HTMLResponse)
async def root(request: Request, session_id: Annotated[Optional[str], Cookie()] = None):
    """Render the login page or redirect to chat if already logged in"""
    if session_id and session_id in active_sessions:
        return RedirectResponse(url="/channels")

    start_background_warmup()
    return render_login(request)


@app.post("/login")
async def login(request: Request, email: Annotated[str, Form()], password: Annotated[str, Form()]):
    """Handle login form submission"""
    # Call login server to authenticate user
    try:
        response = _request_login_server(
            "POST",
            "/token",
            data={"username": email, "password": password},
        )

        if response.status_code != 200:
            # Return to login page with error
            login_response = render_login(
                request,
                get_error_message(response, "Invalid email or password"),
            )
            login_response.status_code = 401
            return login_response

        # Extract token from response
        token_data = response.json()
        access_token = token_data.get("access_token")
        username = token_data.get("username")

        if not access_token or not username:
            login_response = render_login(request, "Authentication error")
            login_response.status_code = 401
            return login_response

        return create_session_response(username, access_token)

    except Exception as e:
        login_response = render_login(request, f"Error: {str(e)}")
        login_response.status_code = 500
        return login_response


@app.get("/register", response_class=HTMLResponse)
async def register_page(request: Request, session_id: Annotated[Optional[str], Cookie()] = None):
    if session_id and session_id in active_sessions:
        return RedirectResponse(url="/channels")

    start_background_warmup()
    return templates.TemplateResponse(
        "register.html", {"request": request, "error_message": ""}
    )


@app.post("/register")
async def register(
    request: Request,
    first_name: Annotated[str, Form()],
    last_name: Annotated[str, Form()],
    email: Annotated[str, Form()],
    password: Annotated[str, Form()],
):
    try:
        register_response = _request_login_server(
            "POST",
            "/users",
            json={
                "first_name": first_name,
                "last_name": last_name,
                "email": email,
                "password": password,
            },
        )

        if register_response.status_code != 200:
            response = templates.TemplateResponse(
                "register.html",
                {
                    "request": request,
                    "error_message": get_error_message(register_response, "Could not create account"),
                },
            )
            response.status_code = 400
            return response

        token_response = _request_login_server(
            "POST",
            "/token",
            data={"username": email, "password": password},
        )
        if token_response.status_code != 200:
            response = render_login(request, "", "Account created. Please log in.")
            return response

        token_data = token_response.json()
        return create_session_response(
            token_data["username"],
            token_data["access_token"],
        )

    except Exception as e:
        response = templates.TemplateResponse(
            "register.html",
            {"request": request, "error_message": f"Error: {str(e)}"},
        )
        response.status_code = 500
        return response


@app.get("/auth/google/login")
async def google_login(request: Request):
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET):
        response = render_login(
            request,
            "Google login is not configured. Set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET.",
        )
        response.status_code = 503
        return response

    clear_stale_oauth_session(request)
    start_background_warmup()
    return await oauth.google.authorize_redirect(request, GOOGLE_REDIRECT_URI)


@app.get("/auth/google/callback")
async def google_callback(request: Request):
    try:
        oauth_error = request.query_params.get("error")
        if oauth_error:
            description = request.query_params.get(
                "error_description", "Sign-in was cancelled or denied."
            )
            login_response = render_login(request, f"Google sign-in failed: {description}")
            login_response.status_code = 401
            return login_response

        token = await oauth.google.authorize_access_token(request)
        user_info = token.get("userinfo")
        if not user_info:
            user_info = await oauth.google.userinfo(token=token)

        if not user_info.get("email"):
            login_response = render_login(
                request, "Google did not provide an email for this account."
            )
            login_response.status_code = 400
            return login_response

        _wake_login_server()

        try:
            response = _request_login_server(
                "POST",
                "/auth/google",
                json={
                    "email": user_info.get("email"),
                    "name": user_info.get("name"),
                    "first_name": user_info.get("given_name"),
                    "last_name": user_info.get("family_name"),
                    "google_sub": user_info.get("sub"),
                },
            )
        except requests.RequestException as e:
            login_response = render_login(
                request,
                f"Could not reach login server at {LOGIN_SERVER_URL}. "
                f"On Render free tier the service may be waking up — wait 30s and try again. ({e})",
            )
            login_response.status_code = 502
            return login_response

        if response.status_code != 200:
            fallback = "Google authentication failed"
            if response.status_code in (502, 503, 504):
                fallback = (
                    f"Login server unavailable ({response.status_code}) at {LOGIN_SERVER_URL}. "
                    f"Open /api/status on this site to diagnose. "
                    f"On Render: confirm login_server is Live, set DATABASE_URL + JWT_PRIVATE_KEY, redeploy."
                )
            elif response.status_code == 503:
                fallback = (
                    "Login server not ready (503). Set JWT_PRIVATE_KEY and JWT_PUBLIC_KEY "
                    "on the login_server service in Render."
                )
            login_response = render_login(
                request,
                get_error_message(response, fallback),
            )
            login_response.status_code = 401
            return login_response

        token_data = response.json()
        return create_session_response(
            token_data["username"],
            token_data["access_token"],
        )

    except OAuthError as e:
        detail = e.error or "oauth_error"
        if detail == "mismatching_state":
            message = (
                "Google sign-in session expired or was interrupted. "
                "Use http://localhost:5004 (not 127.0.0.1), avoid the back button, "
                "and try again in one tab."
            )
        else:
            message = f"Google authentication failed: {detail}"
        login_response = render_login(request, message)
        login_response.status_code = 401
        return login_response
    except Exception as e:
        login_response = render_login(request, f"Google authentication failed: {str(e)}")
        login_response.status_code = 500
        return login_response


@app.get("/channels", response_class=HTMLResponse)
async def channels(request: Request, user_data: dict = Depends(get_current_user)):
    """Render the channels page"""
    return templates.TemplateResponse(
        "channels.html",
        {
            "request": request,
            "username": user_data["username"],
            "token": user_data["token"],
            "websocket_url": WEBSOCKET_CLIENT_URL,
        },
    )


@app.get("/chat/{channel_id}", response_class=HTMLResponse)
async def chat(
    request: Request, channel_id: int, user_data: dict = Depends(get_current_user)
):
    """Render the chat page for a specific channel"""
    return templates.TemplateResponse(
        "chat.html",
        {
            "request": request,
            "username": user_data["username"],
            "token": user_data["token"],
            "websocket_url": WEBSOCKET_CLIENT_URL,
            "channel_id": channel_id,
        },
    )


@app.get("/logout")
async def logout(session_id: Annotated[Optional[str], Cookie()] = None):
    """Handle user logout"""
    if session_id and session_id in active_sessions:
        del active_sessions[session_id]

    response = RedirectResponse(url="/")
    response.delete_cookie(key="session_id")
    return response
