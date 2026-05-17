#
#  channel_requests.py
#  fastapi_kafka
#
#  Created by Xavier Cañadas on 28/4/2025
#  Copyright (c) 2025. All rights reserved.
import json
import os
from urllib.parse import quote

import aiohttp
from fastapi import WebSocket

from .models import ChannelRequest


def _channel_manager_urls() -> dict[str, str]:
    """
    Build channel_manager API URLs from CHANNEL_MANAGER_URL.
    Accepts either the service root or .../channels/ suffix.
    """
    raw = os.getenv("CHANNEL_MANAGER_URL", "http://channel_manager:80/channels/").strip()
    raw = raw.rstrip("/")
    if raw.endswith("/channels"):
        base = raw[: -len("/channels")]
    else:
        base = raw

    return {
        "create": f"{base}/channels",
        "join": f"{base}/channels/join",
        "me": f"{base}/channels/me",
        "messages": f"{base}/channels/messages",
        "by_name": f"{base}/channels",
    }


async def _response_error_message(response: aiohttp.ClientResponse) -> str:
    try:
        body = await response.json()
        detail = body.get("detail")
        if isinstance(detail, str):
            return detail
        if isinstance(detail, list) and detail:
            first = detail[0]
            if isinstance(first, dict):
                loc = ".".join(str(part) for part in first.get("loc", ()))
                msg = first.get("msg", "Invalid request")
                return f"{loc}: {msg}" if loc else msg
    except Exception:
        pass
    text = (await response.text()).strip()
    if text and len(text) < 300:
        return text
    return f"HTTP {response.status}"


async def _send_json(websocket: WebSocket, payload: dict) -> None:
    await websocket.send_text(json.dumps(payload))


_DUPLICATE_CHANNEL_MESSAGE = (
    "This channel name is already taken. Please choose another name."
)


def _is_duplicate_channel_error(status: int, detail: str) -> bool:
    if status != 400:
        return False
    lowered = detail.lower()
    return (
        detail == "duplicate_channel"
        or "already taken" in lowered
        or "already exists" in lowered
    )


async def _proxy_request(
    websocket: WebSocket,
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    *,
    json_body: dict | None = None,
    error_prefix: str,
) -> None:
    try:
        async with session.request(
            method,
            url,
            json=json_body,
            headers={"Content-Type": "application/json"} if json_body else None,
        ) as response:
            if response.status >= 400:
                detail = await _response_error_message(response)
                if _is_duplicate_channel_error(response.status, detail):
                    await _send_json(
                        websocket,
                        {
                            "error": "duplicate_channel",
                            "message": _DUPLICATE_CHANNEL_MESSAGE,
                        },
                    )
                    return
                await _send_json(websocket, {"error": f"{error_prefix}: {detail}"})
                return
            result = await response.json()
            await _send_json(websocket, result)
    except aiohttp.ClientError as error:
        await _send_json(websocket, {"error": f"{error_prefix}: {error}"})


async def send_channel_request(request_str: str, username: str, websocket: WebSocket):
    urls = _channel_manager_urls()

    try:
        data = json.loads(request_str)
        channel_request = ChannelRequest(**data)

        async with aiohttp.ClientSession() as session:
            if channel_request.operation == 0:
                if not channel_request.channel_id:
                    await _send_json(websocket, {"error": "Failed to join channel: channel_id is required"})
                    return
                await _proxy_request(
                    websocket,
                    session,
                    "POST",
                    urls["join"],
                    json_body={
                        "channel_id": channel_request.channel_id,
                        "username": username,
                    },
                    error_prefix="Failed to join channel",
                )

            elif channel_request.operation == 1:
                name = (channel_request.channel_name or "").strip()
                if not name:
                    await _send_json(websocket, {"error": "Failed to create channel: channel name is required"})
                    return
                await _proxy_request(
                    websocket,
                    session,
                    "POST",
                    urls["create"],
                    json_body={
                        "channel_name": name,
                        "channel_description": (channel_request.description or "").strip()
                        or "No description",
                    },
                    error_prefix="Failed to create channel",
                )

            elif channel_request.operation == 2:
                await _proxy_request(
                    websocket,
                    session,
                    "GET",
                    f"{urls['me']}/{quote(username, safe='')}",
                    error_prefix="Failed to load your channels",
                )

            elif channel_request.operation == 3:
                search_name = (channel_request.channel_name or "").strip()
                if not search_name:
                    await _send_json(websocket, {"error": "Failed to search channels: enter a channel name"})
                    return
                await _proxy_request(
                    websocket,
                    session,
                    "GET",
                    f"{urls['by_name']}/{quote(search_name, safe='')}",
                    error_prefix="Failed to search channels",
                )

            elif channel_request.operation == 4:
                if not channel_request.channel_id:
                    await _send_json(websocket, {"error": "Failed to load channel history: channel_id is required"})
                    return
                await _proxy_request(
                    websocket,
                    session,
                    "GET",
                    f"{urls['messages']}/{channel_request.channel_id}",
                    error_prefix="Failed to load channel history",
                )

            else:
                await _send_json(
                    websocket,
                    {"error": f"Unknown operation: {channel_request.operation}"},
                )

    except json.JSONDecodeError as error:
        await _send_json(
            websocket,
            {"error": "Invalid JSON format for channel request. " + str(error)},
        )
    except Exception as error:
        await _send_json(
            websocket,
            {"error": f"Channel request processing failed: {error}"},
        )
