from __future__ import annotations
import json
from typing import Dict
from pathlib import Path


def dump_state_json(path: str,
                    node_id: str,
                    lsdb: Dict[str, Dict[str, float]],
                    routing_table: Dict[str, str]) -> None:
    """
    Guarda un snapshot mínimo del estado a JSON.
    """
    data = {
        "node_id": node_id,
        "lsdb": lsdb,
        "routing_table": routing_table,
    }
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_state_json(path: str) -> Dict:
    """
    Carga snapshot (si existe). Devuelve dict vacío si no existe.
    """
    p = Path(path)
    if not p.exists():
        return {}
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)
