"""
Trading-Notification API 
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import platform
import signal
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic_settings import BaseSettings

import redis.asyncio as aioredis
import websockets
from fastapi import (
    APIRouter,
    Body,
    Depends,
    FastAPI,
    HTTPException,
    Request,
    status,
)
from fastapi.security import (
    APIKeyHeader,
)
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware

# --------------------------------------------------------------------------- #
# 1. Settings – typed, .env-aware                                             #
# --------------------------------------------------------------------------- #


class Settings(BaseSettings):
    REDIS_URL: str = "redis://localhost:6379/0"
    DEFAULT_SYMBOL: str = "BTCUSDT"

    ALERT_BUFFER: float = 0.5  # USD
    CHECK_INTERVAL: int = 5  # s

    MAX_RECONNECT_ATTEMPTS: int = 5
    CIRCUIT_BREAKER_THRESHOLD: int = 5
    CIRCUIT_BREAKER_RESET: int = 60  # s

    NOTIFICATION_RETENTION: int = 604_800  # 7d in s
    CLEANUP_INTERVAL: int = 86_400  # 24h in s

    # auth
    API_KEY: str = "dev-secret"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()

# --------------------------------------------------------------------------- #
# 2. Logging                                                                  #
# --------------------------------------------------------------------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s : %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)
logger = logging.getLogger("notification-api")

# --------------------------------------------------------------------------- #
# 3. Security – simple API-key header                                         #
# --------------------------------------------------------------------------- #

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(key: str | None = Depends(api_key_header)) -> str:
    if key != settings.API_KEY:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid API key")
    # in a real system, key → user_id mapping would live in DB/Redis
    return "default_user"


# --------------------------------------------------------------------------- #
# 4. Models                                                                   #
# --------------------------------------------------------------------------- #


class NotificationStatus(str, Enum):
    UNREAD = "unread"
    READ = "read"


class AlertType(str, Enum):
    ABOVE = "above"
    BELOW = "below"


class NotificationResponse(BaseModel):
    id: str
    alert_id: str
    symbol: str
    alert_type: AlertType
    trigger_price: float
    current_price: float
    timestamp: datetime
    status: NotificationStatus


class AlertCreate(BaseModel):
    symbol: str = Field(default=settings.DEFAULT_SYMBOL, examples=["BTCUSDT"])
    type: AlertType = Field(..., examples=[AlertType.ABOVE])
    price: float = Field(..., gt=0, examples=[50_000.0])


# --------------------------------------------------------------------------- #
# 5. Redis connection – singleton without per-call ping                       #
# --------------------------------------------------------------------------- #


class RedisManager:
    _instance: "RedisManager" | None = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._redis: aioredis.Redis | None = None
        return cls._instance

    async def get(self) -> aioredis.Redis:
        try:
            if self._redis is None:
                self._redis = aioredis.from_url(
                    settings.REDIS_URL,
                    decode_responses=True,
                    socket_keepalive=True,
                    max_connections=20,
                )
            # lightweight health-check
            await self._redis.ping()
            return self._redis
        except Exception:
            # recreate the client on any failure
            self._redis = None
            raise

    async def close(self) -> None:
        if self._redis and not self._redis.closed:
            await self._redis.close()


redis_manager = RedisManager()


async def get_redis() -> aioredis.Redis:
    redis = await redis_manager.get()
    try:
        await redis.ping()
    except Exception as exc:  # pragma: no cover
        logger.error("Redis ping failed: %s", exc)
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "Redis unavailable"
        ) from exc
    return redis


# --------------------------------------------------------------------------- #
# 6. Services                                                                 #
# --------------------------------------------------------------------------- #


class NotificationService:
    def __init__(self, redis: aioredis.Redis):
        self.redis = redis

    async def create(
        self, user: str, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        nid = str(uuid.uuid4())
        data = {
            "id": nid,
            **payload,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": NotificationStatus.UNREAD.value,
        }
        key = f"user:{user}:notifications"
        async with self.redis.pipeline(transaction=True) as pipe:
            (
                pipe.hset(key, nid, json.dumps(data))
                .expire(key, settings.NOTIFICATION_RETENTION, xx=False)
            )
            await pipe.execute()
        return data

    async def list(self, user: str) -> List[Dict[str, Any]]:
        raw = await self.redis.hgetall(f"user:{user}:notifications")
        notif = [json.loads(v) for v in raw.values()]
        return sorted(notif, key=lambda x: x["timestamp"], reverse=True)

    async def mark_read(self, user: str, nid: str) -> Dict[str, Any]:
        key = f"user:{user}:notifications"
        raw = await self.redis.hget(key, nid)
        if raw is None:
            raise HTTPException(404, "Notification not found")
        data = json.loads(raw)
        data["status"] = NotificationStatus.READ.value
        await self.redis.hset(key, nid, json.dumps(data))
        return data

    async def delete(self, user: str, nid: str) -> bool:
        return bool(
            await self.redis.hdel(f"user:{user}:notifications", nid)
        )


class AlertService:
    def __init__(self, redis: aioredis.Redis):
        self.redis = redis

    async def get_all(self, user: str) -> Dict[str, Any]:
        raw = await self.redis.hgetall(f"user:{user}:alerts")
        return {aid: json.loads(v) for aid, v in raw.items()}

    async def create(self, user: str, data: Dict[str, Any]) -> str:
        aid = str(uuid.uuid4())
        full = {
            **data,
            "id": aid,
            "is_active": True,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "triggered": False,
        }
        await self.redis.hset(f"user:{user}:alerts", aid, json.dumps(full))
        return aid

    async def delete(self, user: str, aid: str) -> bool:
        await self.redis.srem(f"user:{user}:triggered_alerts", aid)
        return bool(await self.redis.hdel(f"user:{user}:alerts", aid))


# --------------------------------------------------------------------------- #
# 7. Background dispatcher                                                    #
# --------------------------------------------------------------------------- #


class NotificationDispatcher:
    def __init__(self) -> None:
        self.current_price: float = 0.0
        self.active: bool = True

        self.reconnect_attempts: int = 0
        self.circuit_failures: int = 0
        self.circuit_tripped: bool = False

        self.monitor_task: Optional[asyncio.Task] = None
        self.cleanup_task: Optional[asyncio.Task] = None

    # ------------- price monitor ------------------------------------------ #
    async def monitor_prices(self, symbol: str) -> None:
        uri = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@trade"

        while self.active:
            if self.circuit_tripped:
                await self._wait_circuit_reset()

            try:
                async with websockets.connect(
                    uri,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=10,
                ) as ws:
                    logger.info("WebSocket connected: %s", symbol)
                    self.reconnect_attempts = 0
                    self._reset_circuit()

                    async for msg in ws:
                        data = json.loads(msg)
                        await self.process_price(
                            symbol, float(data["p"])
                        )
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("WebSocket error: %s", exc)
                await self._handle_reconnect()

    async def _handle_reconnect(self) -> None:
        self.reconnect_attempts += 1
        if self.reconnect_attempts > settings.MAX_RECONNECT_ATTEMPTS:
            logger.error("Max reconnect exceeded; stopping monitor")
            self.active = False
            return
        delay = min(2 ** self.reconnect_attempts, 60)
        logger.info("Reconnect in %ds", delay)
        await asyncio.sleep(delay)

    # -------- circuit breaker -------------------------------------------- #
    def _register_failure(self) -> None:
        self.circuit_failures += 1
        if self.circuit_failures >= settings.CIRCUIT_BREAKER_THRESHOLD:
            self.circuit_tripped = True
            logger.error("Circuit breaker tripped")

    def _reset_circuit(self) -> None:
        self.circuit_failures = 0
        self.circuit_tripped = False

    async def _wait_circuit_reset(self) -> None:
        logger.error("Waiting for circuit breaker reset")
        await asyncio.sleep(settings.CIRCUIT_BREAKER_RESET)
        self._reset_circuit()

    # -------------- price handling --------------------------------------- #
    async def process_price(self, symbol: str, price: float) -> None:
        self.current_price = price
        redis = await redis_manager.get()
        alerts = await AlertService(redis).get_all("default_user")

        for aid, alert in alerts.items():
            if not alert.get("is_active", True):
                continue
            if await redis.sismember(
                "user:default_user:triggered_alerts", aid
            ):
                continue
            if self._should_trigger(alert, price):
                await self._trigger(redis, aid, alert, symbol, price)

    @staticmethod
    def _should_trigger(alert: Dict[str, Any], price: float) -> bool:
        threshold = alert["price"]
        if alert["type"] == "above":
            return price >= threshold
        return price <= threshold

    async def _trigger(
        self,
        redis: aioredis.Redis,
        aid: str,
        alert: Dict[str, Any],
        symbol: str,
        price: float,
    ) -> None:
        # atomic pipeline
        async with redis.pipeline(transaction=True) as pipe:
            pipe.sadd("user:default_user:triggered_alerts", aid)
            alert["triggered"] = True
            pipe.hset("user:default_user:alerts", aid, json.dumps(alert))
            await pipe.execute()

        notification = await NotificationService(redis).create(
            "default_user",
            {
                "alert_id": aid,
                "symbol": symbol,
                "alert_type": alert["type"],
                "trigger_price": alert["price"],
                "current_price": price,
            },
        )
        logger.info("Alert triggered → notification %s", notification["id"])

        asyncio.create_task(self._reset_monitor(aid, alert))

    async def _reset_monitor(
        self, aid: str, alert: Dict[str, Any]
    ) -> None:
        redis = await redis_manager.get()
        thresh = alert["price"]
        above = alert["type"] == "above"

        while self.active:
            await asyncio.sleep(settings.CHECK_INTERVAL)
            price = self.current_price

            reset = (
                (above and price < thresh - settings.ALERT_BUFFER)
                or (not above and price > thresh + settings.ALERT_BUFFER)
            )
            if reset:
                async with redis.pipeline() as pipe:
                    pipe.srem("user:default_user:triggered_alerts", aid)
                    alert["triggered"] = False
                    pipe.hset(
                        "user:default_user:alerts", aid, json.dumps(alert)
                    )
                    await pipe.execute()
                logger.info("Alert %s reset", aid)
                break

    # -------------- cleanup ---------------------------------------------- #
    async def cleanup_notifications(self) -> None:
        while self.active:
            try:
                await asyncio.sleep(settings.CLEANUP_INTERVAL)
                redis = await redis_manager.get()
                logger.info("Running notification cleanup")
                # rely on key expiry → nothing to do
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Cleanup error: %s", exc)
                self._register_failure()


dispatcher = NotificationDispatcher()

# --------------------------------------------------------------------------- #
# 8. FastAPI lifespan / graceful shutdown                                     #
# --------------------------------------------------------------------------- #


class SignalWatcher:
    def __init__(self) -> None:
        self.event = asyncio.Event()
        if platform.system() != "Windows":
            for sig in {signal.SIGINT, signal.SIGTERM}:
                asyncio.get_running_loop().add_signal_handler(
                    sig, self.event.set
                )

    async def wait(self) -> None:
        await self.event.wait()


async def _shutdown() -> None:
    dispatcher.active = False
    for task in (dispatcher.monitor_task, dispatcher.cleanup_task):
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
    await redis_manager.close()
    logger.info("Shutdown complete")


@asynccontextmanager
async def lifespan(_: FastAPI):
    watcher = SignalWatcher()

    dispatcher.monitor_task = asyncio.create_task(
        dispatcher.monitor_prices(settings.DEFAULT_SYMBOL)
    )
    dispatcher.cleanup_task = asyncio.create_task(
        dispatcher.cleanup_notifications()
    )
    logger.info("Service started")

    shutdown_task = asyncio.create_task(watcher.wait())
    try:
        yield
    finally:
        shutdown_task.cancel()
        await _shutdown()


# --------------------------------------------------------------------------- #
# 9. API Routers                                                              #
# --------------------------------------------------------------------------- #

api = APIRouter(prefix="/api/v1")


@api.post("/alerts", status_code=201)
async def create_alert(
    payload: AlertCreate = Body(...),
    user: str = Depends(verify_api_key),
    redis: aioredis.Redis = Depends(get_redis),
):
    aid = await AlertService(redis).create(user, payload.model_dump())
    return {"id": aid, **payload.model_dump()}


@api.get("/alerts")
async def get_alerts(
    user: str = Depends(verify_api_key),
    redis: aioredis.Redis = Depends(get_redis),
):
    alerts = await AlertService(redis).get_all(user)
    return list(alerts.values())


@api.delete("/alerts/{alert_id}", status_code=204)
async def delete_alert(
    alert_id: str,
    user: str = Depends(verify_api_key),
    redis: aioredis.Redis = Depends(get_redis),
):
    if not await AlertService(redis).delete(user, alert_id):
        raise HTTPException(404, "Alert not found")


@api.get("/notifications", response_model=List[NotificationResponse])
async def get_notifications(
    user: str = Depends(verify_api_key),
    redis: aioredis.Redis = Depends(get_redis),
):
    notif = await NotificationService(redis).list(user)
    return [NotificationResponse(**n) for n in notif]


@api.patch("/notifications/{nid}/read", response_model=NotificationResponse)
async def mark_read(
    nid: str,
    user: str = Depends(verify_api_key),
    redis: aioredis.Redis = Depends(get_redis),
):
    n = await NotificationService(redis).mark_read(user, nid)
    return NotificationResponse(**n)


@api.delete("/notifications/{nid}", status_code=204)
async def delete_notification(
    nid: str,
    user: str = Depends(verify_api_key),
    redis: aioredis.Redis = Depends(get_redis),
):
    if not await NotificationService(redis).delete(user, nid):
        raise HTTPException(404, "Notification not found")


# --------------------------------------------------------------------------- #
# 10. Health – adds version & uptime                                          #
# --------------------------------------------------------------------------- #

START_TIME = datetime.now(timezone.utc)


@api.get("/health")
async def health(redis: aioredis.Redis = Depends(get_redis)):
    return {
        "redis": "ok",
        "monitoring": dispatcher.active and not dispatcher.circuit_tripped,
        "reconnect_attempts": dispatcher.reconnect_attempts,
        "circuit": {
            "tripped": dispatcher.circuit_tripped,
            "failures": dispatcher.circuit_failures,
        },
        "uptime_s": (datetime.now(timezone.utc) - START_TIME).seconds,
        "version": "1.1.0",
    }


# --------------------------------------------------------------------------- #
# 11. FastAPI application                                                     #
# --------------------------------------------------------------------------- #

app = FastAPI(
    title="Trading Notification API",
    version="1.1.0",
    description="Real-time trading alert & notification service",
    lifespan=lifespan,
    middleware=[
        Middleware(
            CORSMiddleware,
            allow_origins=["*"],  # tighten in prod
            allow_methods=["*"],
            allow_headers=["*"],
        )
    ],
)

app.include_router(api)

# --------------------------------------------------------------------------- #
# 12. Dev entry-point                                                         #
# --------------------------------------------------------------------------- #

if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

