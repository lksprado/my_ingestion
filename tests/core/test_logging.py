import logging

from core.logging import setup_logger


def test_setup_logger_configures_root_once_and_reaches_all_modules(capsys):
    root = logging.getLogger()
    before = list(root.handlers)
    if hasattr(root, "_my_ingestion_configured"):
        delattr(root, "_my_ingestion_configured")
    try:
        setup_logger()
        setup_logger("qualquer")
        added = [h for h in root.handlers if h not in before]
        assert len(added) == 1

        logging.getLogger("pipelines.x.y").info("chegou")
        assert "pipelines.x.y | INFO | chegou" in capsys.readouterr().out
    finally:
        for h in root.handlers[:]:
            if h not in before:
                root.removeHandler(h)
        delattr(root, "_my_ingestion_configured")
