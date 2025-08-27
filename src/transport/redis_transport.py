from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any, AsyncIterator, Dict, Iterable, Optional

import redis.asyncio as redis

from src.utils.log import setup_logger


@dataclass
class RedisSettings:
    host: str
    port: int = 6379
    password: Optional[str] = None
    db: int = 0
    decode_responses: bool = True  # publicar/leer como str (JSON)
    # timeouts
    socket_timeout: float = 10.0
    health_check_interval: float = 15.0


class RedisTransport:
    """
    Capa de transporte Pub/Sub sobre Redis.
    - Conecta a Redis
    - Se suscribe al canal del nodo (my_channel)
    - Publica a canales (unicast) o a varios (broadcast)
    - Entrega un iterador async de mensajes entrantes (JSON o str)

    Uso típico:
        t = RedisTransport(settings, my_channel="sec10.topo1.A", logger_name="A")
        await t.connect()
        async for raw in t.read_loop():
            ...
    """

    def __init__(self, settings: RedisSettings, my_channel: str, logger_name: str = "transport") -> None:
        self.settings = settings
        self.my_channel = my_channel
        self._client: Optional[redis.Redis] = None
        self._pubsub: Optional[redis.client.PubSub] = None
        self._closed = False
        self.log = setup_logger(logger_name)

    # ------------- lifecycle -------------

    async def connect(self) -> None:
        if self._client:
            return

        self._client = redis.Redis(
            host=self.settings.host,
            port=self.settings.port,
            password=self.settings.password,
            db=self.settings.db,
            decode_responses=self.settings.decode_responses,
            socket_timeout=self.settings.socket_timeout,
            health_check_interval=self.settings.health_check_interval,
        )

        # Verifica conexión
        pong = await self._client.ping()
        if not pong:
            raise RuntimeError("Redis PING failed")
        self.log.info(f"Conectado a Redis {self.settings.host}:{self.settings.port}")

        # PubSub y suscripción a mi canal
        self._pubsub = self._client.pubsub()
        await self._pubsub.subscribe(self.my_channel)
        self.log.info(f"Suscrito a canal propio: {self.my_channel}")

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            if self._pubsub:
                await self._pubsub.unsubscribe(self.my_channel)
                await self._pubsub.close()
        finally:
            if self._client:
                await self._client.close()
        self.log.info("Transporte Redis cerrado")

    # ------------- publish -------------

    async def publish(self, channel: str, message: str | Dict[str, Any]) -> int:
        """
        Publica un mensaje (str o dict). Si es dict, se serializa a JSON.
        Devuelve cantidad de suscriptores a los que se entregó.
        """
        if not self._client:
            raise RuntimeError("Transport no conectado")
        if isinstance(message, dict):
            payload = json.dumps(message, ensure_ascii=False)
        else:
            payload = message
        subscribers = await self._client.publish(channel, payload)
        self.log.debug(f"PUBLISH → {channel} ({subscribers} subs): {payload}")
        return subscribers

    async def publish_json(self, channel: str, payload: Dict[str, Any]) -> int:
        return await self.publish(channel, payload)

    async def broadcast(self, neighbor_channels: Iterable[str], message: str | Dict[str, Any]) -> None:
        """
        Publica el mismo mensaje a múltiples canales (vecinos).
        No espera a que cada publish termine secuencialmente, sino en paralelo.
        """
        tasks = [self.publish(ch, message) for ch in neighbor_channels]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=False)

    # ------------- receive -------------

    async def read_loop(self, poll_interval: float = 0.05) -> AsyncIterator[str]:
        """
        Iterador async que entrega payloads crudos (str) recibidos por PubSub.
        - Devuelve sólo los mensajes 'message' (no 'subscribe', etc.)
        - El consumidor puede parsear JSON cuando lo necesite.
        """
        if not self._pubsub:
            raise RuntimeError("Transport no conectado")

        while not self._closed:
            try:
                msg = await self._pubsub.get_message(ignore_subscribe_messages=True, timeout=poll_interval)
                if msg is None:
                    continue
                # msg: {'type':'message','pattern':None,'channel':'sec10.topo1.A','data':'...'}
                data = msg.get("data")
                if data is None:
                    continue
                yield data
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self.log.error(f"Error en read_loop: {exc}")
                await asyncio.sleep(0.2)

    # ------------- helpers -------------

    @staticmethod
    def channel_name(section: str, topo: str, node: str) -> str:
        """
        Helper para mantener el patrón SECTION.TOPO.NODE
        """
        return f"{section}.{topo}.{node}"
