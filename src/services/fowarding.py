from __future__ import annotations
import asyncio
import json
from typing import Any, Dict, Callable, Awaitable, Optional, Iterable, Set

from src.protocol.schema import PacketFactory, HelloPacket, InfoPacket, UserMessagePacket, BasePacket
from src.storage.state import State
from src.transport.redis_transport import RedisTransport
from src.utils.log import setup_logger


class ForwardingService:
    """
    Servicio de forwarding:
      - Lee del canal propio (RedisTransport.read_loop)
      - Parsea, valida y aplica reglas por tipo (hello/info/message)
      - Reenvía según TTL, headers (trail anti-ciclo), routing_table y flooding controlado
      - Delega la actualización de LSR al callback on_info_async(payload)

    Requiere:
      state: State (neighbors, lsdb, routing_table, seen_cache)
      transport: RedisTransport (publish/broadcast)
      my_id: str
      neighbor_map: dict node_id -> channel_name (para publicar a vecinos)
      on_info_async: async fn(from_id: str, info_payload: dict) -> None
                     (La implementa tu servicio LSR para actualizar LSDB y recálculo de rutas)
    """

    def __init__(
        self,
        state: State,
        transport: RedisTransport,
        my_id: str,
        neighbor_map: Dict[str, str],
        on_info_async: Callable[[str, Dict[str, Any]], Awaitable[None]],
        hello_timeout_sec: float = 20.0,
        logger_name: Optional[str] = None,
    ) -> None:
        self.state = state
        self.transport = transport
        self.my_id = my_id
        self.neighbor_map = dict(neighbor_map)  # id -> canal
        self.on_info_async = on_info_async
        self.hello_timeout_sec = hello_timeout_sec
        self.log = setup_logger(logger_name or f"FWD-{my_id}")

        # tarea principal
        self._runner_task: Optional[asyncio.Task] = None
        # control de apagado
        self._stopping = asyncio.Event()

    # ---------------- Lifecycle ----------------

    async def start(self) -> None:
        """
        Inicia el bucle de lectura y manejo de paquetes.
        """
        if self._runner_task and not self._runner_task.done():
            return
        self.log.info("ForwardingService iniciado")
        self._runner_task = asyncio.create_task(self._run())

        # tarea periódica para purgar seen_cache y detectar vecinos caídos
        asyncio.create_task(self._housekeeping())

    async def stop(self) -> None:
        """
        Solicita detener el servicio.
        """
        self._stopping.set()
        if self._runner_task:
            self._runner_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._runner_task

    # ---------------- Internals ----------------

    async def _run(self) -> None:
        """
        Bucle principal: consume mensajes crudos del canal propio,
        parsea JSON → PacketFactory → maneja por tipo.
        """
        async for raw in self.transport.read_loop():
            if self._stopping.is_set():
                break

            try:
                data = json.loads(raw)
            except Exception:
                self.log.warning(f"Descartado (JSON inválido): {raw[:120]}…")
                continue

            try:
                pkt = PacketFactory.parse_obj(data)
            except Exception as e:
                self.log.warning(f"Descartado (schema inválido): {e} - raw={data}")
                continue

            await self._handle_packet(pkt)

    async def _housekeeping(self) -> None:
        """
        Tarea periódica: purga cache de vistos, detecta vecinos caídos por falta de HELLO.
        """
        try:
            while not self._stopping.is_set():
                await asyncio.sleep(5.0)
                self.state.purge_seen()

                # vecinos “muertos” por inactividad de HELLO
                dead = await self.state.dead_neighbors(self.hello_timeout_sec)
                for n in dead:
                    self.log.warning(f"Vecino sin HELLO: {n} (posible caída)")
                # Nota: la remoción efectiva del vecino del LSDB la haría tu LSR,
                # ya sea aquí o en un servicio de timers (para emitir nuevo INFO).
        except asyncio.CancelledError:
            pass

    # ---------------- Dispatch por tipo ----------------

    async def _handle_packet(self, pkt: BasePacket) -> None:
        """
        Aplica reglas comunes (de-dupe, TTL, anti-ciclo) y deriva por tipo.
        """
        # de-dupe por msg_id
        if pkt.msg_id and self.state.is_seen(pkt.msg_id):
            self.log.debug(f"VISTO (de-dupe) {pkt.type} id={pkt.msg_id}")
            return
        if pkt.msg_id:
            self.state.mark_seen(pkt.msg_id)

        # anti-ciclo: si ya pasé por mí, lo descarto
        if pkt.seen_cycle(self.my_id):
            self.log.debug(f"CICLO detectado: {pkt.type} trace={pkt.trace_id}")
            return

        # TTL: si llega con 0, solo lo consumiría destino (MESSAGE) o control, pero no reenvía
        if pkt.ttl <= 0 and pkt.type in ("info", "message"):
            self.log.debug(f"TTL=0 descartado: {pkt.type} id={pkt.msg_id}")
            return

        # derivar a handlers específicos
        if isinstance(pkt, HelloPacket):
            await self._on_hello(pkt)
        elif isinstance(pkt, InfoPacket):
            await self._on_info(pkt)
        elif isinstance(pkt, UserMessagePacket):
            await self._on_message(pkt)
        else:
            # Desconocido pero válido (BasePacket) → descartar
            self.log.debug(f"Tipo no manejado: {pkt.type}")

    # ---------------- Handlers ----------------

    async def _on_hello(self, pkt: HelloPacket) -> None:
        """
        HELLO: no se retransmite. Marca actividad del vecino.
        """
        from_node = pkt.from_
        await self.state.touch_hello(from_node)
        self.log.info(f"[HELLO] de {from_node} (trace={pkt.trace_id})")

        # Opcional: si llega un HELLO de alguien que no tengo mapeado como vecino,
        # puedes decidir agregarlo dinámicamente o ignorarlo.
        # if from_node not in self.neighbor_map:
        #     self.log.info(f"HELLO de no-vecino {from_node} (ignorado en forwarding)")

    async def _on_info(self, pkt: InfoPacket) -> None:
        """
        INFO: actualizar LSDB vía callback y retransmitir a vecinos con TTL-- y headers++.
        """
        origin = pkt.from_
        # 1) Actualiza LSDB / dispara recálculo LSR
        try:
            # payload puede ser LSP o “tabla hacia destinos”, según acuerdo de tu grupo
            await self.on_info_async(origin, pkt.payload)
        except Exception as e:
            self.log.error(f"Error en on_info_async: {e}")

        # 2) Retransmitir a vecinos (excepto al “prev hop” si podemos inferirlo)
        pkt_out = pkt.with_decremented_ttl().with_appended_hop(self.my_id)
        if pkt_out.ttl <= 0:
            return

        prev_hop: Optional[str] = pkt.headers[-1] if pkt.headers else None
        await self._broadcast_to_neighbors(pkt_out, exclude={prev_hop} if prev_hop else set())
        self.log.debug(f"[INFO] retransmitido trace={pkt.trace_id} ttl={pkt_out.ttl}")

    async def _on_message(self, pkt: UserMessagePacket) -> None:
        """
        MESSAGE:
          - Si soy destino → entregar (print/log).
          - Si tengo next_hop → enviar unicast al canal del next_hop.
          - Si no hay ruta → flooding controlado a vecinos (TTL--, headers++), con de-dupe.
        """
        dst = pkt.to
        if dst == self.my_id:
            self._deliver(pkt)
            return

        # Intentar ruteo por tabla
        next_hop = await self.state.get_next_hop(dst)
        if next_hop:
            ch = self.neighbor_map.get(next_hop)
            if ch:
                pkt_out = pkt.with_decremented_ttl().with_appended_hop(self.my_id)
                if pkt_out.ttl > 0:
                    await self.transport.publish_json(ch, pkt_out.to_publish_dict())
                    self.log.info(f"[MSG] {pkt.from_}→{dst} via {next_hop} trace={pkt.trace_id}")
                    return

        # Fallback: flooding controlado a vecinos (evitar rebotar al prev_hop)
        prev_hop: Optional[str] = pkt.headers[-1] if pkt.headers else None
        pkt_out = pkt.with_decremented_ttl().with_appended_hop(self.my_id)
        if pkt_out.ttl <= 0:
            self.log.debug(f"[MSG] TTL agotado, descartar trace={pkt.trace_id}")
            return

        await self._broadcast_to_neighbors(pkt_out, exclude={prev_hop} if prev_hop else set())
        self.log.info(f"[MSG-FLOOD] {pkt.from_}→{dst} (sin ruta) trace={pkt.trace_id}")

    # ---------------- Helpers ----------------

    async def _broadcast_to_neighbors(self, pkt: BasePacket, exclude: Set[str] = set()) -> None:
        """
        Envía un paquete a todos los vecinos directos, excluyendo algunos IDs (p.ej., prev_hop).
        """
        # Solo a vecinos conocidos en neighbor_map
        targets = [nid for nid in self.neighbor_map.keys() if nid not in exclude and nid != self.my_id]
        channels = [self.neighbor_map[nid] for nid in targets]
        if not channels:
            return
        await self.transport.broadcast(channels, pkt.to_publish_dict())

    def _deliver(self, pkt: UserMessagePacket) -> None:
        """
        Entrega local del mensaje (aquí lo dejamos en log/stdout).
        """
        body_preview = pkt.payload if isinstance(pkt.payload, str) else json.dumps(pkt.payload)
        self.log.info(f"[DELIVERED] {pkt.from_} → {self.my_id} :: {body_preview[:200]}")
