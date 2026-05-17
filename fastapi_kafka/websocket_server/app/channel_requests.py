#
#  channel_requests.py
#  fastapi_kafka
#
#  Created by Xavier Cañadas on 28/4/2025
#  Copyright (c) 2025. All rights reserved.
import json
import os

import aiohttp
from fastapi import WebSocket

from .models import ChannelRequest


async def _response_error_message(response: aiohttp.ClientResponse) -> str:
    try:
        body = await response.json()
        detail = body.get("detail")
        if isinstance(detail, str):
            return detail
    except Exception:
        pass
    return f"Request failed (HTTP {response.status})"


async def _send_json(websocket: WebSocket, payload: dict) -> None:
    await websocket.send_text(json.dumps(payload))


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
                await _send_json(websocket, {"error": f"{error_prefix}: {detail}"})
                return
            result = await response.json()
            await _send_json(websocket, result)
    except aiohttp.ClientError as error:
        await _send_json(websocket, {"error": f"{error_prefix}: {error}"})


async def send_channel_request(request_str: str, username: str, websocket: WebSocket):
    channel_server_url = os.getenv(
        "CHANNEL_MANAGER_URL", "http://channel_manager:80/channels/"
    )
    if not channel_server_url.endswith("/"):
        channel_server_url += "/"

    try:
        data = json.loads(request_str)
        channel_request = ChannelRequest(**data)

        async with aiohttp.ClientSession() as session:
            if channel_request.operation == 0:
                await _proxy_request(
                    websocket,
                    session,
                    "POST",
                    channel_server_url + "join",
                    json_body={
                        "channel_id": channel_request.channel_id,
                        "username": username,
                    },
                    error_prefix="Failed to join channel",
                )

            elif channel_request.operation == 1:
                await _proxy_request(
                    websocket,
                    session,
                    "POST",
                    channel_server_url,
                    json_body={
                        "channel_name": channel_request.channel_name,
                        "channel_description": channel_request.description or "",
                    },
                    error_prefix="Failed to create channel",
                )

            elif channel_request.operation == 2:
                await _proxy_request(
                    websocket,
                    session,
                    "GET",
                    channel_server_url + "me/" + username,
                    error_prefix="Failed to load your channels",
                )

            elif channel_request.operation == 3:
                await _proxy_request(
                    websocket,
                    session,
                    "GET",
                    channel_server_url + channel_request.channel_name,
                    error_prefix="Failed to search channels",
                )

            elif channel_request.operation == 4:
                await _proxy_request(
                    websocket,
                    session,
                    "GET",
                    channel_server_url + "messages/" + str(channel_request.channel_id),
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
