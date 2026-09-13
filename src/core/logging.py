"""Logger único do monorepo.

Regra: módulos usam ``logging.getLogger(__name__)``; o entrypoint (``run_source``
ou o ``__main__`` de um script-exceção) chama ``setup_logger()`` uma vez. Como a
configuração vai no logger raiz, todo ``INFO`` de qualquer módulo chega ao
console — sem depender de hierarquia de nomes.

Uso:
    from core import setup_logger
    logger = setup_logger()            # entrypoint
    logger = logging.getLogger(__name__)   # módulo
"""

import logging
import sys
from pathlib import Path

FORMAT = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
_MARK = "_my_ingestion_configured"


def setup_logger(
    name: str | None = None,
    level: int = logging.INFO,
    log_file: Path | str | None = None,
) -> logging.Logger:
    """Configura o logger raiz (console e, opcionalmente, arquivo rotativo).

    Idempotente: chamadas repetidas não duplicam handlers. Devolve
    ``logging.getLogger(name)`` (o raiz se ``name`` for None).
    """
    root = logging.getLogger()
    if not getattr(root, _MARK, False):
        root.setLevel(level)
        formatter = logging.Formatter(FORMAT, datefmt=DATE_FORMAT)

        console = logging.StreamHandler(sys.stdout)
        console.setFormatter(formatter)
        root.addHandler(console)

        if log_file:
            from logging.handlers import RotatingFileHandler

            log_file = Path(log_file)
            log_file.parent.mkdir(parents=True, exist_ok=True)
            file_handler = RotatingFileHandler(
                log_file, maxBytes=10_000_000, backupCount=5
            )
            file_handler.setFormatter(formatter)
            root.addHandler(file_handler)

        setattr(root, _MARK, True)

    return logging.getLogger(name) if name else root
