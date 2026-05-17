#
#  database.py
#  fastapi_kafka
#
#  Created by Xavier Cañadas on 24/4/2025
#  Copyright (c) 2025. All rights reserved.
import os
from typing import Annotated, Sequence

from bson import ObjectId
from fastapi import Depends
from pymongo import MongoClient, DESCENDING
from sqlalchemy import Select
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, create_engine, select
from sqlmodel.sql._expression_select_cls import _T

from .models import Channel, MessageCollection, User, UserChannels

# Init postgres database
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://root:password@relational_database:5432/chatdb",
)
engine = create_engine(DATABASE_URL, echo=os.getenv("SQL_ECHO", "").lower() == "true")


def get_session():
    with Session(engine) as session:
        yield session


SessionDep = Annotated[Session, Depends(get_session)]

# Init mongodb
MONGODB_URL = os.environ.get("MONGODB_URL", "mongodb://root:password@mongodb:27017/")
client = MongoClient(MONGODB_URL)
db = client.chatdb
messages_collection = db.messages


def get_channels_from_user(username: str, session: SessionDep) -> Sequence[_T] | list[Channel]:
    statement: Select = (
        select(Channel).join(UserChannels).where(UserChannels.username == username)
    )
    return session.exec(statement).all()


def get_channels_by_name(channel_name: str, session: SessionDep) -> Sequence[_T] | list[Channel]:
    statement: Select = select(Channel).where(Channel.channel_name == channel_name)
    return session.exec(statement).all()


def create_channel(channel_name: str, description: str, session: SessionDep) -> Channel:
    name = (channel_name or "").strip()
    if not name:
        raise ValueError("channel_name is required")

    channel_description = (description or "").strip() or "No description"

    channel = Channel(channel_name=name, description=channel_description)
    session.add(channel)
    try:
        session.commit()
        session.refresh(channel)
        return channel
    except IntegrityError:
        session.rollback()
        raise ValueError("duplicate_channel") from None


def join_channel(channel_id: int, username: str, session: Session) -> UserChannels:
    if not channel_id:
        raise ValueError("channel_id is required")
    if not username:
        raise ValueError("username is required")

    user = session.get(User, username)
    if not user:
        raise ValueError(
            "Your account is not in this database. On Render, set the same "
            "DATABASE_URL on login_server and channel_manager, then sign in again."
        )

    channel = session.get(Channel, channel_id)
    if not channel:
        raise ValueError(f"Channel {channel_id} does not exist. Search by exact channel name.")

    existing = session.exec(
        select(UserChannels).where(
            UserChannels.channel_id == channel_id,
            UserChannels.username == username,
        )
    ).first()
    if existing:
        raise ValueError("You are already in this channel.")

    user_channel = UserChannels(username=username, channel_id=channel_id)
    session.add(user_channel)
    try:
        session.commit()
        session.refresh(user_channel)
        return user_channel
    except IntegrityError as error:
        session.rollback()
        raise ValueError(
            "Could not join channel. Confirm login_server and channel_manager "
            "use the same DATABASE_URL."
        ) from error


def _normalize_stored_message(doc: dict) -> dict:
    """Prepare a MongoDB document for API/history responses."""
    normalized = dict(doc)
    stored_id = normalized.get("_id")
    if stored_id is None:
        normalized.pop("_id", None)
    elif isinstance(stored_id, ObjectId):
        normalized["_id"] = str(stored_id)
    return normalized


def get_channel_messages_history(channel_id: int, limit: int = 500) -> MessageCollection:
    channel_messages = [
        _normalize_stored_message(doc)
        for doc in messages_collection.find({"channel_id": channel_id})
        .sort("timestamp", DESCENDING)
        .limit(limit)
    ]
    channel_messages.reverse()
    return MessageCollection(messages=channel_messages)
