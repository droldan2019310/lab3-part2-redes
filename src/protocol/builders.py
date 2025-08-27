from __future__ import annotations
import os
from typing import Dict, Any, Union
from src.protocol.schema import (
    HelloPacket,
    InfoPacket,
    UserMessagePacket,
    PacketFactory,
)

# Defaults desde entorno (con fallback)
DEFAULT_TTL = int(os.getenv("TTL_DEFAULT", "5"))
PROTO = os.getenv("PROTO", "lsr")


def _base_headers() -> list[str]:
    # Si quieres “sembrar” algo en headers al originar (normalmente vacío)
    return []


def build_hello(my_id: str, ttl: int | None = None) -> HelloPacket:
    """
    Crea un paquete HELLO para presentar vecinos.
    'to' es 'broadcast' por definición del grupo.
    """
    pkt = HelloPacket(
        proto=PROTO,
        type="hello",
        **{"from": my_id},  # alias de 'from_'
        to="broadcast",
        ttl=DEFAULT_TTL if ttl is None else ttl,
        headers=_base_headers(),
        payload=""
    )
    return PacketFactory.ensure_trace(pkt, my_id)  # agrega trace_id si no existe


def build_info(my_id: str,
               view: Dict[str, Union[int, float]],
               ttl: int | None = None) -> InfoPacket:
    """
    Crea un paquete INFO con la vista local.
    'view' puede ser:
      - LSP (enlaces/costos): p.ej. {"B":1,"C":3}
      - Tabla hacia destinos: p.ej. {"A":3,"C":1,"J":2}
    """
    pkt = InfoPacket(
        proto=PROTO,
        type="info",
        **{"from": my_id},
        to="broadcast",
        ttl=DEFAULT_TTL if ttl is None else ttl,
        headers=_base_headers(),
        payload=dict(view or {})
    )
    return PacketFactory.ensure_trace(pkt, my_id)


def build_message(my_id: str,
                  dst: str,
                  body: Union[str, Dict[str, Any]] = "",
                  ttl: int | None = None) -> UserMessagePacket:
    """
    Crea un paquete MESSAGE (unicast lógico). Si 'dst' no es alcanzable, el
    forwarding puede optar por flooding controlado.
    """
    pkt = UserMessagePacket(
        proto=PROTO,
        type="message",
        **{"from": my_id},
        to=dst,
        ttl=DEFAULT_TTL if ttl is None else ttl,
        headers=_base_headers(),
        payload=body
    )
    return PacketFactory.ensure_trace(pkt, my_id)
