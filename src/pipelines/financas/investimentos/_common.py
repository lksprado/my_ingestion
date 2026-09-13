"""Helpers dos pipelines de investimentos."""

import logging

from core import PipelineConfig

logger = logging.getLogger(__name__)


def reset_bronze(cfg: PipelineConfig) -> None:
    """Apaga os CSVs do bronze antes de regravar.

    Com ``load: files`` cada arquivo do diretório vira uma tabela; o bronze é
    inteiramente derivado do landing (full refresh), então um CSV antigo que
    sobrasse viraria uma tabela fantasma.
    """
    old = list(cfg.bronze_dir.glob("*.csv"))
    for f in old:
        f.unlink()
    if old:
        logger.info(f"🧹 {len(old)} CSV(s) antigo(s) removido(s) de {cfg.bronze_dir}")
