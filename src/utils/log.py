import logging
import sys

# Formato de log
LOG_FORMAT = "[%(asctime)s] [%(levelname)s] %(name)s: %(message)s"

def setup_logger(name: str = "node", level: str = "INFO") -> logging.Logger:
    """
    Crea un logger con el nivel y formato dado.
    Ejemplo de uso:
        logger = setup_logger("NodeA", "DEBUG")
        logger.info("Arrancando nodo A")
    """
    logger = logging.getLogger(name)
    logger.setLevel(level.upper())

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(handler)

    # Evita duplicación de handlers si se llama varias veces
    logger.propagate = False

    return logger
