from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Set, List, Optional, Tuple
import time
import asyncio


class TTLCache:
    """
    Cache de 'msg_id' vistos con TTL (para de-dupe de INFO/MESSAGE).
    """
    def __init__(self, ttl_seconds: int = 120) -> None:
        self.ttl = ttl_seconds
        self._store: Dict[str, float] = {}

    def add(self, key: str) -> None:
        self._store[key] = time.time() + self.ttl

    def __contains__(self, key: str) -> bool:
        exp = self._store.get(key)
        if exp is None:
            return False
        if exp < time.time():
            # expirado → retirar
            self._store.pop(key, None)
            return False
        return True

    def purge(self) -> None:
        now = time.time()
        expired = [k for k, exp in self._store.items() if exp < now]
        for k in expired:
            self._store.pop(k, None)


@dataclass
class NeighborInfo:
    cost: float = 1.0
    last_hello_ts: float = field(default_factory=lambda: 0.0)


@dataclass
class State:
    """
    Estado compartido del nodo:
    - neighbors: información de enlaces directos y último HELLO recibido
    - lsdb: base de datos de estado de enlaces (LSR)
    - routing_table: destino -> next_hop
    - seen_cache: ids de mensajes vistos (de-dupe)
    """
    node_id: str
    neighbors: Dict[str, NeighborInfo] = field(default_factory=dict)
    lsdb: Dict[str, Dict[str, float]] = field(default_factory=dict)  # por nodo: {vecino: costo}
    routing_table: Dict[str, str] = field(default_factory=dict)      # dst -> next_hop
    seen_cache: TTLCache = field(default_factory=lambda: TTLCache(120))

    # lock para uso con asyncio (lecturas concurrentes + escrituras controladas)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    # -----------------------------
    # Vecinos directos
    # -----------------------------
    async def set_neighbors(self, initial: List[Tuple[str, float]]) -> None:
        """
        Establece vecinos directos (p.ej., a partir de topo.json).
        initial: lista de (neighbor_id, cost)
        """
        async with self._lock:
            self.neighbors = {n: NeighborInfo(cost=c) for n, c in initial}
            # también refleja mi fila en LSDB
            self.lsdb[self.node_id] = {n: c for n, c in initial}

    async def add_neighbor(self, neighbor_id: str, cost: float = 1.0) -> None:
        async with self._lock:
            self.neighbors[neighbor_id] = NeighborInfo(cost=cost)
            self.lsdb.setdefault(self.node_id, {})[neighbor_id] = cost

    async def remove_neighbor(self, neighbor_id: str) -> None:
        async with self._lock:
            self.neighbors.pop(neighbor_id, None)
            if self.node_id in self.lsdb:
                self.lsdb[self.node_id].pop(neighbor_id, None)

    async def touch_hello(self, neighbor_id: str, now: Optional[float] = None) -> None:
        """Actualiza timestamp del último HELLO de un vecino (para detectar caída)."""
        ts = now if now is not None else time.time()
        async with self._lock:
            info = self.neighbors.get(neighbor_id)
            if info:
                info.last_hello_ts = ts

    async def dead_neighbors(self, timeout_sec: float) -> List[str]:
        """Devuelve vecinos que no envían HELLO hace > timeout_sec."""
        now = time.time()
        async with self._lock:
            out = []
            for n, info in self.neighbors.items():
                if info.last_hello_ts and (now - info.last_hello_ts) > timeout_sec:
                    out.append(n)
            return out

    async def update_link_cost(self, neighbor_id: str, cost: float) -> None:
        """Actualiza el costo del enlace directo (p.ej., RTT medio)."""
        async with self._lock:
            if neighbor_id in self.neighbors:
                self.neighbors[neighbor_id].cost = cost
                self.lsdb.setdefault(self.node_id, {})[neighbor_id] = cost

    # -----------------------------
    # LSDB (Link State Database)
    # -----------------------------
    async def update_lsdb(self, origin: str, links: Dict[str, float]) -> None:
        """
        Actualiza LSDB con el anuncio (INFO/LSP) de 'origin':
        links: {neighbor: cost}
        """
        async with self._lock:
            self.lsdb[origin] = dict(links)

    async def get_lsdb_snapshot(self) -> Dict[str, Dict[str, float]]:
        async with self._lock:
            # copia superficial para lectura segura
            return {k: dict(v) for k, v in self.lsdb.items()}

    async def build_graph(self) -> Dict[str, Dict[str, float]]:
        """
        Construye un grafo no dirigido a partir de la LSDB.
        Si en LSDB A->B existe pero B->A no, igual lo refleja (coste simétrico simple).
        """
        async with self._lock:
            graph: Dict[str, Dict[str, float]] = {}
            for u, edges in self.lsdb.items():
                graph.setdefault(u, {})
                for v, w in edges.items():
                    graph[u][v] = w
                    graph.setdefault(v, {})
                    # Si no hay costo inverso explícito, asume mismo costo (puedes ajustar)
                    graph[v].setdefault(u, w)
            return graph

    # -----------------------------
    # Tabla de ruteo
    # -----------------------------
    async def set_routing_table(self, table: Dict[str, str]) -> None:
        """Sobrescribe la tabla completa: dst -> next_hop."""
        async with self._lock:
            self.routing_table = dict(table)

    async def get_next_hop(self, dst: str) -> Optional[str]:
        async with self._lock:
            return self.routing_table.get(dst)

    async def get_routing_snapshot(self) -> Dict[str, str]:
        async with self._lock:
            return dict(self.routing_table)

    # -----------------------------
    # seen_cache (de-dupe)
    # -----------------------------
    def mark_seen(self, msg_id: str) -> None:
        self.seen_cache.add(msg_id)

    def is_seen(self, msg_id: str) -> bool:
        return msg_id in self.seen_cache

    def purge_seen(self) -> None:
        self.seen_cache.purge()
