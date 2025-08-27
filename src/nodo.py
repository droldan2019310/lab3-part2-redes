from __future__ import annotations
import os
import json
import asyncio
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional

from dotenv import load_dotenv

from src.storage.state import State
from src.transport.redis_transport import RedisTransport, RedisSettings
from src.services.fowarding import ForwardingService
from src.services.routing_lsr import RoutingLSRService, LSRConfig
from src.protocol.builders import build_hello, build_info, build_message
from src.utils.log import setup_logger


def _load_json(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"No existe el archivo: {path}")
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)


class Node:
    """
    Nodo LSR con transporte Redis y servicios de Forwarding/LSR.

    - Lee .env para: REDIS_HOST, REDIS_PORT, REDIS_PWD, SECTION, TOPO, NODE, NAMES_PATH, TOPO_PATH, etc.
    - Carga configs: names.json (id->canal), topo.json (vecinos)
    - Inicializa:
        State (vecinos directos, LSDB)
        RedisTransport (canal propio)
        RoutingLSRService (LSDB + Dijkstra + INFO)
        ForwardingService (recepción y reenvío)
    - Envía HELLO e INFO iniciales
    """

    def __init__(self, env_path: Optional[str] = None) -> None:
        if env_path:
            load_dotenv(env_path)
        else:
            load_dotenv()

        # ── Env ──────────────────────────────────────────────────────────────
        self.section = os.getenv("SECTION", "sec10")
        self.topo_id = os.getenv("TOPO", "topo1")
        self.my_id = os.getenv("NODE", "A")
        self.names_path = os.getenv("NAMES_PATH", "./configs/names.json")
        self.topo_path = os.getenv("TOPO_PATH", "./configs/topo.json")
        self.hello_interval = float(os.getenv("HELLO_INTERVAL_SEC", "5"))
        self.info_interval = float(os.getenv("INFO_INTERVAL_SEC", "12"))
        self.hello_timeout = float(os.getenv("HELLO_TIMEOUT_SEC", "20"))
        self.log_level = os.getenv("LOG_LEVEL", "INFO")

        # ── Redis settings ───────────────────────────────────────────────────
        self.redis_settings = RedisSettings(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            password=os.getenv("REDIS_PWD", None),
            db=0,
            decode_responses=True,
        )

        # ── Logger ───────────────────────────────────────────────────────────
        self.log = setup_logger(f"NODE-{self.my_id}", self.log_level)

        # ── State y servicios (se crean en bootstrap) ────────────────────────
        self.state: Optional[State] = None
        self.transport: Optional[RedisTransport] = None
        self.forwarding: Optional[ForwardingService] = None
        self.lsr: Optional[RoutingLSRService] = None

        # ── Configs cargadas ─────────────────────────────────────────────────
        self.names_cfg: Dict[str, str] = {}       # node_id -> channel
        self.topo_cfg: Dict[str, List[str]] = {}  # node_id -> [neighbors]
        self.neighbor_ids: List[str] = []
        self.neighbor_map: Dict[str, str] = {}    # neighbor_id -> channel

        # ── Tasks locales ───────────────────────────────────────────────────
        self._hello_task: Optional[asyncio.Task] = None

    # ─────────────────────────────────────────────────────────────────────────

    def _my_channel(self) -> str:
        """
        Canal propio conforme a names.json.
        Si no existe, lo infiere como SECTION.TOPO.NODE
        """
        ch = self.names_cfg.get(self.my_id)
        if ch:
            return ch
        return f"{self.section}.{self.topo_id}.{self.my_id}"

    def _load_configs(self) -> None:
        names = _load_json(self.names_path)
        topo = _load_json(self.topo_path)

        if names.get("type") != "names" or "config" not in names:
            raise ValueError("names.json inválido: falta {type:'names', config:{...}}")
        if topo.get("type") != "topo" or "config" not in topo:
            raise ValueError("topo.json inválido: falta {type:'topo', config:{...}}")

        self.names_cfg = dict(names["config"])
        self.topo_cfg = dict(topo["config"])

        self.neighbor_ids = list(self.topo_cfg.get(self.my_id, []))
        self.neighbor_map = {nid: self.names_cfg[nid] for nid in self.neighbor_ids if nid in self.names_cfg}

        if not self.neighbor_map:
            self.log.warning("Este nodo no tiene vecinos mapeados en names.json/topo.json")

        self.log.info(f"Vecinos de {self.my_id}: {self.neighbor_ids}")
        self.log.info(f"Canal propio: {self._my_channel()}")

    async def _bootstrap_services(self) -> None:
        # Estado inicial (vecinos directos con costo 1.0)
        self.state = State(node_id=self.my_id)
        await self.state.set_neighbors([(n, 1.0) for n in self.neighbor_ids])

        # Transporte
        self.transport = RedisTransport(self.redis_settings, my_channel=self._my_channel(), logger_name=self.my_id)
        await self.transport.connect()

        # LSR
        self.lsr = RoutingLSRService(
            state=self.state,
            transport=self.transport,
            my_id=self.my_id,
            neighbor_map=self.neighbor_map,
            cfg=LSRConfig(
                hello_timeout_sec=self.hello_timeout,
                info_interval_sec=self.info_interval,
                on_change_debounce_sec=0.4,
                advertise_links_from_neighbors_table=True,  # LSP clásico
            ),
            logger_name=f"LSR-{self.my_id}",
        )
        await self.lsr.start()

        # Forwarding con callback → LSR
        async def _on_info(origin: str, view: dict) -> None:
            # Forwarding dispara esto al recibir INFO
            await self.lsr.on_info(origin, view)

        self.forwarding = ForwardingService(
            state=self.state,
            transport=self.transport,
            my_id=self.my_id,
            neighbor_map=self.neighbor_map,
            on_info_async=_on_info,
            hello_timeout_sec=self.hello_timeout,
            logger_name=f"FWD-{self.my_id}",
        )
        await self.forwarding.start()

        # HELLO/INFO iniciales
        await self._emit_initial_control_packets()

        # HELLO periódico (INFO periódico lo maneja LSR internamente)
        self._hello_task = asyncio.create_task(self._periodic_hello())

    async def _emit_initial_control_packets(self) -> None:
        assert self.transport is not None
        # HELLO inicial
        hello = build_hello(self.my_id).to_publish_dict()
        await self.transport.broadcast(self.neighbor_map.values(), hello)
        # INFO inicial (mis enlaces directos)
        initial_links = {n: 1.0 for n in self.neighbor_ids}
        info = build_info(self.my_id, initial_links).to_publish_dict()
        await self.transport.broadcast(self.neighbor_map.values(), info)
        self.log.info("HELLO/INFO iniciales enviados")

    async def _periodic_hello(self) -> None:
        """
        Emite HELLO a vecinos cada HELLO_INTERVAL_SEC.
        """
        assert self.transport is not None
        try:
            while True:
                await asyncio.sleep(self.hello_interval)
                pkt = build_hello(self.my_id).to_publish_dict()
                await self.transport.broadcast(self.neighbor_map.values(), pkt)
        except asyncio.CancelledError:
            return

    # ─────────────────────────────────────────────────────────────────────────
    # API pública
    # ─────────────────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Carga configs, arranca transporte y servicios.
        """
        self._load_configs()
        await self._bootstrap_services()
        self.log.info(f"Nodo {self.my_id} iniciado.")

    async def stop(self) -> None:
        """
        Detiene timers y cierra transporte.
        """
        if self._hello_task:
            self._hello_task.cancel()
            try:
                await self._hello_task
            except asyncio.CancelledError:
                pass

        if self.lsr:
            await self.lsr.stop()

        if self.forwarding:
            await self.forwarding.stop()

        if self.transport:
            await self.transport.close()

        self.log.info(f"Nodo {self.my_id} detenido.")

    async def send_message(self, dst: str, body: Any = "hola") -> None:
        """
        Envía un MESSAGE a 'dst' usando ruteo (o flooding si no hay ruta).
        Publica al canal del next-hop si existe; si no, broadcast a vecinos.
        """
        assert self.transport is not None and self.state is not None

        pkt = build_message(self.my_id, dst, body).to_publish_dict()

        # intentar ruta conocida
        next_hop = await self.state.get_next_hop(dst)
        if next_hop:
            ch = self.neighbor_map.get(next_hop)
            if ch:
                await self.transport.publish_json(ch, pkt)
                self.log.info(f"[CLI] MESSAGE {self.my_id}→{dst} via {next_hop}")
                return

        # sin ruta → flooding controlado (Forwarding hará de-dupe por msg_id)
        await self.transport.broadcast(self.neighbor_map.values(), pkt)
        self.log.info(f"[CLI] MESSAGE {self.my_id}→{dst} (flooding)")

# ─────────────────────────────────────────────────────────────────────────────

# Ejecutable sencillo: levantar un nodo leyendo .env
if __name__ == "__main__":
    async def _main():
        node = Node()
        await node.start()
        # Mantener vivo hasta Ctrl+C
        try:
            await asyncio.Event().wait()
        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
            await node.stop()

    asyncio.run(_main())
