
# ### COMO UTILIZAR
# # uso simples
# from utils.log import logger

# # uso customizado
# from utils.log import setup_logger
# import logging

# logger = setup_logger(
#     name="meuapp",
#     level=logging.WARNING,
#     log_to_file=True
# )



# src/utils/log.py
import logging
import sys
from pathlib import Path

def setup_logger(
    name: str = __name__,
    level: int = logging.INFO,
    log_to_file: bool = False,
    log_dir: str = "logs"
) -> logging.Logger:
    """
    Configura e retorna um logger.
    
    Args:
        name: Nome do logger
        level: Nível de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_to_file: Se True, também salva logs em arquivo
        log_dir: Diretório para salvar os arquivos de log
    """
    logger = logging.getLogger(name)
    
    # Evita duplicação de handlers
    if logger.handlers:
        return logger
    
    logger.setLevel(level)
    
    # Formato dos logs
    formatter = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Handler para console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Handler para arquivo (opcional)
    if log_to_file:
        log_path = Path(log_dir)
        log_path.mkdir(exist_ok=True)
        
        from logging.handlers import RotatingFileHandler
        file_handler = RotatingFileHandler(
            log_path / "app.log",
            maxBytes=10_000_000,  # 10MB
            backupCount=5
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

# Logger padrão pronto para usar
logger = setup_logger()

