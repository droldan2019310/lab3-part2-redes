from __future__ import annotations
import asyncio
import argparse
import signal
from typing import Optional

from src.nodo import Node


async def _run_node(env_path: Optional[str], send_dst: Optional[str], send_body: str) -> None:
    node = Node(env_path=env_path)
    await node.start()

    # Si pidieron enviar un mensaje al arrancar
    if send_dst:
        await node.send_message(send_dst, send_body)

    # Espera hasta Ctrl+C
    stop_event = asyncio.Event()

    def _graceful_stop(*_):
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _graceful_stop)
        except NotImplementedError:
            # Windows no soporta signal handlers en ProactorEventLoop
            pass

    await stop_event.wait()
    await node.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Runner para nodo LSR con Redis Pub/Sub"
    )
    parser.add_argument(
        "--env",
        dest="env_path",
        default=None,
        help="Ruta al archivo .env (si no, usa ./.env)"
    )
    parser.add_argument(
        "--send",
        dest="send_dst",
        default=None,
        help="Destino del mensaje (ID del nodo). Si se omite, no envía nada al iniciar."
    )
    parser.add_argument(
        "--body",
        dest="send_body",
        default="hola",
        help="Cuerpo del mensaje a enviar con --send (por defecto: 'hola')"
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(_run_node(args.env_path, args.send_dst, args.send_body))


if __name__ == "__main__":
    main()
