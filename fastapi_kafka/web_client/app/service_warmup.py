#
#  service_warmup.py — wake Render microservices on login (built-in, no UptimeRobot required)
#
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import requests

logger = logging.getLogger(__name__)

LOGIN_SERVER_URL = os.getenv("LOGIN_SERVER_URL", "http://login_server")
CHANNEL_MANAGER_URL = os.getenv("CHANNEL_MANAGER_URL", "")
WARMUP_WEBSOCKET_URL = os.getenv("WARMUP_WEBSOCKET_URL", "")
WEBSOCKET_CLIENT_URL = os.getenv("WEBSOCKET_CLIENT_URL", "")
WARMUP_MAX_SECONDS = int(os.getenv("WARMUP_MAX_SECONDS", "90"))
WARMUP_PING_TIMEOUT = int(os.getenv("WARMUP_PING_TIMEOUT", "15"))

_warmup_lock = threading.Lock()
_warmup_thread: threading.Thread | None = None
_last_warmup_result: dict[str, bool] | None = None


def _login_server_url(path: str) -> str:
    return f"{LOGIN_SERVER_URL.rstrip('/')}{path}"


def _channel_manager_base_url() -> str | None:
    raw = CHANNEL_MANAGER_URL.strip().rstrip("/")
    if not raw:
        return None
    if raw.endswith("/channels"):
        return raw[: -len("/channels")]
    return raw


def _websocket_health_url() -> str | None:
    if WARMUP_WEBSOCKET_URL.strip():
        return WARMUP_WEBSOCKET_URL.strip().rstrip("/") + "/health"

    ws_url = WEBSOCKET_CLIENT_URL.strip()
    if not ws_url or "localhost" in ws_url or "127.0.0.1" in ws_url:
        return None

    normalized = ws_url.replace("wss://", "https://").replace("ws://", "http://")
    parsed = urlparse(normalized)
    if not parsed.netloc:
        return None
    return f"{parsed.scheme}://{parsed.netloc}/health"


def warmup_targets() -> list[tuple[str, str]]:
    """
    Every backend the app needs after login.
    Configure on web_client in Render: LOGIN_SERVER_URL, CHANNEL_MANAGER_URL,
    WARMUP_WEBSOCKET_URL (or public WEBSOCKET_CLIENT_URL on Render).
    """
    targets: list[tuple[str, str]] = []

    if LOGIN_SERVER_URL.strip():
        targets.append(("login_server", _login_server_url("/health")))

    channel_base = _channel_manager_base_url()
    if channel_base:
        targets.append(("channel_manager", f"{channel_base}/health"))

    websocket_health = _websocket_health_url()
    if websocket_health:
        targets.append(("websocket_server", websocket_health))

    for index, url in enumerate(os.getenv("WARMUP_URLS", "").split(",")):
        url = url.strip()
        if url:
            targets.append((f"service_{index}", url))

    return targets


def _ping_service(url: str) -> bool:
    try:
        response = requests.get(url, timeout=WARMUP_PING_TIMEOUT)
        return response.status_code < 500
    except requests.RequestException:
        return False


def wake_service(name: str, url: str) -> bool:
    deadline = time.time() + WARMUP_MAX_SECONDS
    while time.time() < deadline:
        if _ping_service(url):
            logger.info("Warmup OK: %s (%s)", name, url)
            return True
        time.sleep(2)
    logger.warning("Warmup failed: %s (%s)", name, url)
    return False


def wake_all_services() -> dict[str, bool]:
    targets = warmup_targets()
    if not targets:
        logger.warning(
            "No warmup targets. Set LOGIN_SERVER_URL, CHANNEL_MANAGER_URL, "
            "and WARMUP_WEBSOCKET_URL on web_client in Render."
        )
        return {}

    results: dict[str, bool] = {}
    with ThreadPoolExecutor(max_workers=max(len(targets), 1)) as pool:
        futures = {pool.submit(wake_service, name, url): name for name, url in targets}
        for future in as_completed(futures):
            name = futures[future]
            results[name] = future.result()
    return results


def start_background_warmup() -> None:
    """Start waking services without blocking the HTTP response (login page HTML)."""
    global _warmup_thread, _last_warmup_result

    def _run():
        global _last_warmup_result
        _last_warmup_result = wake_all_services()

    with _warmup_lock:
        if _warmup_thread and _warmup_thread.is_alive():
            return
        _warmup_thread = threading.Thread(target=_run, daemon=True, name="service-warmup")
        _warmup_thread.start()


def warmup_status_response() -> dict:
    targets = warmup_targets()
    services = wake_all_services()
    missing_env = []
    if not CHANNEL_MANAGER_URL.strip():
        missing_env.append("CHANNEL_MANAGER_URL")
    if "onrender.com" not in LOGIN_SERVER_URL and LOGIN_SERVER_URL.startswith("http://login"):
        missing_env.append("LOGIN_SERVER_URL (still Docker default)")
    if not _websocket_health_url():
        missing_env.append("WARMUP_WEBSOCKET_URL or public WEBSOCKET_CLIENT_URL")

    return {
        "ready": all(services.values()) if services else False,
        "services": services,
        "targets": {name: url for name, url in targets},
        "missing_env_on_web_client": missing_env,
    }
