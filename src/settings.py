"""Configuração central do monorepo, carregada do .env da raiz.

Uso: ``from settings import settings``.
Campos sem default são obrigatórios — a importação falha rápido se o .env
estiver incompleto, evitando que pipelines criem diretórios/conexões errados.
"""

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

# Ancorado na raiz do repo (src/settings.py -> ../), para que os pipelines
# rodem de qualquer diretório, não só da raiz.
REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = REPO_ROOT / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    env: str = "local"

    # Data lake local e seeds do dbt (repo externo the_dw)
    lake_root: Path
    seeds_root: Path

    # Postgres (destino raw.*)
    db_host: str
    db_port: int = 5432
    db_name: str
    db_user: str
    db_password: str

    # Pipeline energia/solar
    apsystems_user: str | None = None
    apsystems_password: str | None = None

    # Pipeline financas/google_finance
    google_credentials_file: Path | None = None

    @property
    def db_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )


settings = Settings()
