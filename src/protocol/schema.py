from __future__ import annotations
from typing import Any, Dict, List, Literal, Optional, Union
from pydantic import BaseModel, Field, field_validator
from datetime import datetime, timezone
import json  # <-- para normalizar payload string JSON
from src.utils.ids import generate_msg_id, generate_trace_id

# Tipos permitidos en el protocolo "lsr"
PacketType = Literal["hello", "info", "message"]

class BasePacket(BaseModel):
    proto: Literal["lsr", "flooding"] = Field(default="lsr")  # admite ambos si quieres usar flooding
    type: PacketType
    from_: str = Field(alias="from", min_length=1)
    to: str
    ttl: int = Field(ge=0, le=64, default=5)  # 0 = descartable inmediatamente

    # Acepta lista (tu formato) o dict con 'path' (formato externo)
    headers: Union[List[str], Dict[str, Any]] = Field(default_factory=list)
    payload: Any = None

    # Metadatos de tracing
    msg_id: str = Field(default_factory=generate_msg_id)
    timestamp: float = Field(default_factory=lambda: datetime.now(tz=timezone.utc).timestamp())
    trace_id: Optional[str] = None

    class Config:
        populate_by_name = True  # permite usar 'from'
        str_strip_whitespace = True
        validate_assignment = True

    @field_validator("headers")
    @classmethod
    def _normalize_headers(cls, v):
        """
        Soporta:
          - lista: ["A","B","C"] → recorte a 8
          - dict: {"msg_id":"..","seq":9,"path":[...]} → usa 'path' si existe, recorte a 8
        """
        if isinstance(v, dict):
            path = v.get("path", [])
            if isinstance(path, list):
                return path[-8:]
            return []
        if isinstance(v, list):
            return v[-8:]
        return []

    @field_validator("to")
    @classmethod
    def _normalize_to(cls, v: str) -> str:
        if (v or "").lower() == "broadcast":
            return "broadcast"
        return v

    def to_publish_dict(self) -> Dict[str, Any]:
        """Dict apto para publicar como JSON (manteniendo alias 'from')."""
        return self.model_dump(by_alias=True)

    def with_decremented_ttl(self) -> "BasePacket":
        """Devuelve una copia con TTL-1 (sin bajar de 0)."""
        new_ttl = max(0, (self.ttl or 0) - 1)
        data = self.model_dump(by_alias=True)
        data["ttl"] = new_ttl
        return PacketFactory.parse_obj(data)

    def with_appended_hop(self, node_id: str) -> "BasePacket":
        """Agrega mi id al final del trail y recorta si excede."""
        data = self.model_dump(by_alias=True)
        hdrs = data.get("headers", []) or []

        # si vinieron como dict, sacar 'path'
        if isinstance(hdrs, dict):
            hdrs = hdrs.get("path", [])
        if not isinstance(hdrs, list):
            hdrs = []

        hdrs.append(node_id)
        data["headers"] = hdrs[-8:]
        return PacketFactory.parse_obj(data)

    def seen_cycle(self, node_id: str) -> bool:
        """True si ya pasé por este node_id (detectar ciclo)."""
        hdrs = self.headers
        if isinstance(hdrs, dict):
            hdrs = hdrs.get("path", [])
        return isinstance(hdrs, list) and node_id in hdrs


# Paquetes específicos (te permiten más validaciones si las necesitas)
class HelloPacket(BasePacket):
    type: Literal["hello"]

    @field_validator("to")
    @classmethod
    def _hello_to_must_be_broadcast(cls, v: str) -> str:
        # Por definición del grupo: HELLO es broadcast a vecinos directos (no retransmitir)
        if v != "broadcast":
            raise ValueError("HELLO must use to='broadcast'")
        return v


class InfoPacket(BasePacket):
    type: Literal["info"]
    # payload puede ser LSP o la “tabla hacia destinos” acordada.
    # Acepta dict o string JSON (de otros grupos).
    payload: Union[Dict[str, Any], str]

    @field_validator("payload")
    @classmethod
    def _normalize_info_payload(cls, v):
        """
        Soporta:
          - dict directo: {"B":1,"D":1}
          - dict con "neighbors": {"origin":"A","seq":9,"neighbors":{"B":1,"D":1}, ...}
          - string JSON de cualquiera de los dos
        """
        # si viene como string JSON → parsear
        if isinstance(v, str):
            try:
                v = json.loads(v)
            except Exception:
                return {}

        # si no es dict a este punto → vacío
        if not isinstance(v, dict):
            return {}

        # si trae 'neighbors' y es dict → usarlo como vista
        if "neighbors" in v and isinstance(v["neighbors"], dict):
            return dict(v["neighbors"])

        # si ya es mapa destino->costo, devolver tal cual
        return v


class UserMessagePacket(BasePacket):
    type: Literal["message"]
    payload: Union[str, Dict[str, Any], None] = ""


# Fábrica/Parser genérico
class PacketFactory:
    @staticmethod
    def parse_obj(obj: Dict[str, Any]) -> BasePacket:
        """
        Recibe un dict (JSON) y devuelve el modelo adecuado.
        Lanza ValidationError si el paquete no cumple.
        """
        t = (obj.get("type") or "").lower()
        if t == "hello":
            return HelloPacket.model_validate(obj)
        if t == "info":
            return InfoPacket.model_validate(obj)
        if t == "message":
            return UserMessagePacket.model_validate(obj)
        # Si no reconoce el tipo, valida como BasePacket para error claro
        return BasePacket.model_validate(obj)

    @staticmethod
    def ensure_trace(packet: BasePacket, node_id: str) -> BasePacket:
        """Asegura que tenga trace_id, útil al originar paquetes."""
        if not packet.trace_id:
            data = packet.model_dump(by_alias=True)
            data["trace_id"] = generate_trace_id(node_id)
            return PacketFactory.parse_obj(data)
        return packet
