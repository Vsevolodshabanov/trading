from __future__ import annotations

import json
import logging

import redis


logger = logging.getLogger(__name__)


class EventBus:
    def __init__(self, redis_url: str | None) -> None:
        self._client = redis.from_url(redis_url) if redis_url else None

    def publish(self, channel: str, payload: dict[str, object]) -> None:
        if self._client is None:
            return
        try:
            self._client.publish(channel, json.dumps(payload, default=str))
        except redis.RedisError:
            logger.exception("Failed to publish event to redis")

    def ping(self) -> bool:
        if self._client is None:
            return False
        try:
            return bool(self._client.ping())
        except redis.RedisError:
            logger.exception("Redis ping failed")
            return False
