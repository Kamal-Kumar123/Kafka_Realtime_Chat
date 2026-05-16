#
#  kafka_consumer.py — Kafka message consumer (merged for single-process deploy)
#
import asyncio
import json
import logging
import os
import threading
from collections.abc import Awaitable, Callable
from typing import Any, Sequence

import aiohttp
from confluent_kafka import Consumer, KafkaException
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from sqlalchemy import Select
from sqlmodel import Session, create_engine, select
from sqlmodel.sql._expression_select_cls import _T

from .kafka_config import build_kafka_config
from .models import Message, MessageRequest, UserChannels
from .redis_client import get_redis_client

logger = logging.getLogger(__name__)

MESSAGE_CONSUMER_CONFIG = build_kafka_config(group_id="websocket-message-producer")
redis_instance = get_redis_client()

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://root:password@relational_database:5432/chatdb"
)
engine = create_engine(DATABASE_URL, echo=False)

MONGO_URL = os.environ.get("MONGO_URL", "mongodb://root:password@mongodb:27017/")
mongo_client = MongoClient(MONGO_URL)
messages_collection = mongo_client.chatdb.messages

message_consumer = Consumer(MESSAGE_CONSUMER_CONFIG)

_running = False
_consumer_thread: threading.Thread | None = None
_event_loop: asyncio.AbstractEventLoop | None = None
_deliver_to_user: Callable[[Message, str, str | None], Awaitable[None]] | None = None


def get_channel_users(channel_id: int) -> Sequence[_T] | list[Any]:
    with Session(engine) as session:
        try:
            statement: Select = select(UserChannels.username).where(
                UserChannels.channel_id == channel_id
            )
            return session.exec(statement).all()
        except Exception as e:
            logger.error(e)
            return []


def get_user_websocket_server(username: str) -> str | None:
    return redis_instance.hget("active_connections", username)


async def send_message_http(message: Message, username: str, websocket_server_url: str):
    message_request = MessageRequest(message=message, username=username)
    header = {"Content-Type": "application/json"}
    data = message_request.model_dump()

    scheme = os.getenv("WEBSOCKET_MESSAGE_SCHEME", "http")
    if websocket_server_url.startswith("http://") or websocket_server_url.startswith(
        "https://"
    ):
        base = websocket_server_url.rstrip("/")
        url = f"{base}/message"
    else:
        url = f"{scheme}://{websocket_server_url}/message"

    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, json=data, headers=header) as response:
                response.raise_for_status()
                if response.status != 200:
                    logger.error(await response.text())
        except Exception as e:
            logger.error(f"Failed to send message: {e}")


async def store_message(message: Message):
    try:
        message_data = {
            "message_id": message.message_id,
            "channel_id": message.channel_id,
            "timestamp": message.timestamp,
            "username": message.username,
            "message": message.message,
        }
        try:
            result = messages_collection.insert_one(message_data)
        except DuplicateKeyError:
            messages_collection.delete_one({"_id": None})
            result = messages_collection.insert_one(message_data)
        logger.info(f"Message stored in MongoDB with _id: {result.inserted_id}")
    except Exception as e:
        logger.error(f"Failed to store message: {e}")


async def process_message(message: Message):
    usernames = get_channel_users(message.channel_id)
    logger.info(
        f"Distributing message to channel {message.channel_id} users: {usernames}"
    )

    for username in usernames:
        if message.username == username:
            continue
        try:
            websocket_server = get_user_websocket_server(username)
            if _deliver_to_user:
                await _deliver_to_user(message, username, websocket_server)
        except Exception as e:
            logger.error(f"Failed to send message to {username}: {e}")

    await store_message(message)


def _consumer_loop():
    global _running
    try:
        message_consumer.subscribe(["messages"])
        logger.info("Kafka message consumer started (embedded in websocket server)")

        while _running:
            new_message = message_consumer.poll(timeout=1.0)

            if new_message is None:
                continue

            if new_message.error():
                raise KafkaException(new_message.error())

            try:
                message_info = json.loads(new_message.value())
                logger.info(f"Message info: {message_info}")
                if _event_loop and _event_loop.is_running():
                    future = asyncio.run_coroutine_threadsafe(
                        process_message(Message(**message_info)), _event_loop
                    )
                    future.result(timeout=60)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON format: {e}")
            except Exception as e:
                logger.error(f"Error processing message: {e}")

    except KafkaException as e:
        logger.error(f"Kafka consumer error: {e}")
    finally:
        message_consumer.close()
        logger.info("Kafka message consumer closed")


def start_kafka_consumer(
    deliver_to_user: Callable[[Message, str, str | None], Awaitable[None]],
    loop: asyncio.AbstractEventLoop,
):
    global _running, _consumer_thread, _event_loop, _deliver_to_user
    _deliver_to_user = deliver_to_user
    _event_loop = loop
    _running = True
    _consumer_thread = threading.Thread(target=_consumer_loop, daemon=True)
    _consumer_thread.start()


def stop_kafka_consumer():
    global _running, _consumer_thread
    _running = False
    if _consumer_thread is not None:
        _consumer_thread.join(timeout=10)
        _consumer_thread = None
