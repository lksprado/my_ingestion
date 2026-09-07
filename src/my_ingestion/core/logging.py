"""Logger único do monorepo — substitui as cópias de log.py e os basicConfig soltos.

Uso:
    from my_ingestion.core.logging import setup_logger
    logger = setup_logger(__name__)
"""

import logging
import sys
from pathlib import Path

FORMAT = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logger(
    name: str = "my_ingestion",
    level: int = logging.INFO,
    log_file: Path | str | None = None,
) -> logging.Logger:
    """Configura e retorna um logger com handler de console (e arquivo, opcional).

    Idempotente: chamadas repetidas com o mesmo nome não duplicam handlers.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(level)
    formatter = logging.Formatter(FORMAT, datefmt=DATE_FORMAT)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    logger.addHandler(console)

    if log_file:
        from logging.handlers import RotatingFileHandler

        log_file = Path(log_file)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(log_file, maxBytes=10_000_000, backupCount=5)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
