from __future__ import annotations
import asyncio
from typing import Dict, Tuple, Optional, List
from dataclasses import dataclass, field
import time

from src.storage.state import State
from src.transport.redis_transport import RedisTransport
from src.protocol.builders import build_info
from src.utils.log import setup_logger


def _dijkstra_next_hops(
    graph: Dict[str, Dict[str, float]],
    src: str
) -> Dict[str, str]:
    """
    Dijkstra que devuelve únicamente el 'next_hop' por destino, no la ruta completa.
    graph: {u: {v: w, ...}, ...}
    Retorna: {dst: next_hop}
    """
    import math
    dist: Dict[str, float] = {n: math.inf for n in graph.keys()}
    prev: Dict[str, Optional[str]] = {n: None for n in graph.keys()}
    visited: Dict[str, bool] = {n: False for n in graph.keys()}

    dist[src] = 0.0

    # cola simple O(V^2) suficiente para el lab
    for _ in range(len(graph)):
        # elegir el no visitado con menor distancia
        u = None
        best = math.inf
        for n in graph.keys():
            if not visited[n] and dist[n] < best:
                u = n
                best = dist[n]
        if u is None:
            break
        visited[u] = True

        # relajar vecinos
        for v, w in graph[u].items():
            if visited.get(v):
                continue
            alt = dist[u] + float(w)
            if alt < dist[v]:
                dist[v] = alt
                prev[v] = u

    # reconstruir next_hop: para cada dst, seguir prev[] hacia atrás
    next_hop: Dict[str, str] = {}
    for dst in graph.keys():
        if dst == src:
            continue
        # reconstruir primer salto
        # sube desde dst hasta src, guardando el penúltimo
        cur = dst
        if prev[cur] is None:
            continue  # inalcanzable
        while prev[cur] is not None and prev[cur] != src:
            cur = prev[cur]
        if prev[cur] == src:
            next_hop[dst] = cur
    return next_hop


@dataclass
class LSRConfig:
    hello_timeout_sec: float = 20.0
    info_interval_sec: float = 12.0
    on_change_debounce_sec: float = 0.4   # retrasa un poco para acumular cambios
    advertise_links_from_neighbors_table: bool = True
    """
    Si True: el INFO anuncia mis enlaces directos (LSP clásico) usando State.neighbors.
    Si False: el INFO anunciará una 'tabla hacia destinos' si ya la tienes (no recomendado
    para LSR puro, pero lo dejo por compatibilidad con el acuerdo de tu grupo).
    """


class RoutingLSRService:
    """
    Servicio de routing LSR:
      - Mantiene LSDB y recalcula rutas (Dijkstra)
      - Emite INFO periódicamente y on-change
      - Detecta vecinos caídos por timeout de HELLO y anuncia cambios

    Interfaz pública:
      - start()/stop()
      - on_info(from_id, links/view): integrar con ForwardingService
      - maybe_mark_topology_changed(): para disparar recálculo/anuncio

    Dependencias:
      - state: State
      - transport: RedisTransport
      - my_id: str
      - neighbor_map: {node_id: channel_name}
    """

    def __init__(
        self,
        state: State,
        transport: RedisTransport,
        my_id: str,
        neighbor_map: Dict[str, str],
        cfg: Optional[LSRConfig] = None,
        logger_name: Optional[str] = None,
    ) -> None:
        self.state = state
        self.transport = transport
        self.my_id = my_id
        self.neighbor_map = dict(neighbor_map)
        self.cfg = cfg or LSRConfig()
        self.log = setup_logger(logger_name or f"LSR-{my_id}")

        # control
        self._stopping = asyncio.Event()
        self._loop_task: Optional[asyncio.Task] = None
        self._ticker_task: Optional[asyncio.Task] = None
        self._debounce_task: Optional[asyncio.Task] = None

        # versión local de cambios (para evitar anuncios vacíos)
        self._last_advertised_view: Dict[str, float] = {}
        self._last_recalc_ts: float = 0.0

    # ------------- Lifecycle -------------

    async def start(self) -> None:
        if self._loop_task and not self._loop_task.done():
            return
        self.log.info("RoutingLSRService iniciado")

        # Ticker INFO periódico
        self._ticker_task = asyncio.create_task(self._periodic_info())

        # Loop de vigilancia de vecinos caídos (por HELLO timeout) + recálculo
        self._loop_task = asyncio.create_task(self._watchdog())

    async def stop(self) -> None:
        self._stopping.set()
        for task in (self._ticker_task, self._loop_task, self._debounce_task):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    # ------------- Integración con Forwarding -------------

    async def on_info(self, origin: str, view: Dict[str, float]) -> None:
        """
        Llamado por ForwardingService cuando llega un INFO.
        'view' es el contenido de payload; en LSR clásico debe ser LSP de 'origin':
            {"neighbor1": cost1, "neighbor2": cost2, ...}
        """
        await self.state.update_lsdb(origin, view)
        self.log.debug(f"LSDB actualizado por INFO de {origin}: {view}")
        await self._debounced_recompute_and_advertise()

    async def maybe_mark_topology_changed(self) -> None:
        """
        Útil cuando detectas caída/alta de vecino (p. ej., watchdog) o cambio de costo.
        """
        await self._debounced_recompute_and_advertise()

    # ------------- Internals -------------

    async def _periodic_info(self) -> None:
        """
        Emite INFO periódicos con la vista local.
        """
        try:
            while not self._stopping.is_set():
                await asyncio.sleep(self.cfg.info_interval_sec)
                try:
                    await self._advertise_info()
                except Exception as e:
                    self.log.error(f"Error en periodic_info: {e}")
        except asyncio.CancelledError:
            pass

    async def _watchdog(self) -> None:
        """
        Vigila vecinos “muertos” por falta de HELLO y refresca LSDB/rutas si cambia algo.
        """
        try:
            while not self._stopping.is_set():
                await asyncio.sleep(5.0)
                dead = await self.state.dead_neighbors(self.cfg.hello_timeout_sec)
                if not dead:
                    continue

                # Si hay vecinos sin HELLO, retíralos de mi fila en LSDB
                changed = False
                for n in dead:
                    # solo si realmente lo tenía
                    my_links = (await self.state.get_lsdb_snapshot()).get(self.my_id, {})
                    if n in my_links:
                        await self.state.remove_neighbor(n)
                        # al remover vecino directo, ya reflejamos en LSDB (State.remove_neighbor)
                        changed = True
                        self.log.warning(f"Retiro enlace {self.my_id}—{n} por timeout de HELLO")

                if changed:
                    await self._debounced_recompute_and_advertise()
        except asyncio.CancelledError:
            pass

    async def _debounced_recompute_and_advertise(self) -> None:
        """
        Debounce pequeño para agrupar cambios cercanos (por ej. múltiples INFO seguidos).
        """
        # cancela el debounce previo si existe
        if self._debounce_task and not self._debounce_task.done():
            self._debounce_task.cancel()
            try:
                await self._debounce_task
            except asyncio.CancelledError:
                pass

        async def _job():
            await asyncio.sleep(self.cfg.on_change_debounce_sec)
            await self._recompute_routes()
            await self._advertise_info()

        self._debounce_task = asyncio.create_task(_job())

    async def _recompute_routes(self) -> None:
        """
        Recalcula next-hop por destino usando Dijkstra sobre la LSDB agregada.
        """
        graph = await self.state.build_graph()
        if self.my_id not in graph:
            # asegúrate de que exista mi nodo en el grafo aunque no tenga enlaces
            graph[self.my_id] = {}

        table = _dijkstra_next_hops(graph, self.my_id)
        await self.state.set_routing_table(table)
        self._last_recalc_ts = time.time()

        self.log.info(f"Tabla de ruteo actualizada ({len(table)} destinos)")

    async def _advertise_info(self) -> None:
        """
        Construye y emite INFO según configuración:
          - LSP clásico: mis enlaces directos (State.neighbors)
          - (Compat) “tabla hacia destinos”: routing_table en pesos uniformes
        Solo envía si hay cambios respecto al último anuncio (para evitar ruido).
        """
        if self.cfg.advertise_links_from_neighbors_table:
            # anunciar mis enlaces directos (LSP de mi nodo)
            snapshot = await self.state.get_lsdb_snapshot()
            my_links = snapshot.get(self.my_id, {})  # {neighbor: cost}
            view = dict(my_links)
        else:
            # compat: anunciar una “tabla hacia destinos” (no es LSR puro, úsalo si tu grupo lo acordó)
            routing = await self.state.get_routing_snapshot()
            # asigna costo 1 a cada destino alcanzable si no tienes distancias
            view = {dst: 1.0 for dst in routing.keys()}

        # no anunciar si no hay cambios
        if view == self._last_advertised_view:
            return
        self._last_advertised_view = dict(view)

        pkt = build_info(self.my_id, view)
        payload = pkt.to_publish_dict()
        # broadcast a todos los vecinos directos
        channels = [self.neighbor_map[nid] for nid in self.neighbor_map.keys() if nid != self.my_id]
        if channels:
            await self.transport.broadcast(channels, payload)
            self.log.debug(f"[LSR-INFO] anunciado: {view}")
