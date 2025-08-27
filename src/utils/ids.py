import uuid
import time


def generate_msg_id() -> str:
    """
    Genera un identificador único para cada mensaje.
    Usa UUID4 para evitar colisiones entre nodos.
    """
    return str(uuid.uuid4())


def generate_trace_id(node_id: str) -> str:
    """
    Genera un identificador de traza que combina el nodo y un timestamp.
    Sirve para seguir un flujo de mensajes.
    """
    ts = int(time.time() * 1000)  # milisegundos
    return f"{node_id}-{ts}-{uuid.uuid4().hex[:6]}"
