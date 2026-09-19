"""Configuração central do monorepo, carregada do .env da raiz.

Uso: ``from settings import settings``.
Campos sem default são obrigatórios — a importação falha rápido se o .env
estiver incompleto, evitando que pipelines criem diretórios/conexões errados.

Destino no Postgres por ambiente
--------------------------------
São dois ambientes: ``dev`` (execução local) e ``prod`` (Airflow). ``ENV``
escolhe tanto o bloco ``environments`` dos YAMLs quanto o perfil de conexão
``DB__<ENV>__*`` (``DB__DEV__HOST``, ``DB__DEV__NAME``...). O perfil ativo sai em
``settings.db_target``; ``PostgresClient()`` sem argumentos usa ele.
Guard-rail: em ``ENV=dev`` o banco tem que ser ``analytics_dev``. Perfis
inativos podem ficar em branco no .env.
"""

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL

# Ancorado na raiz do repo (src/settings.py -> ../), para que os pipelines
# rodem de qualquer diretório, não só da raiz.
REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = REPO_ROOT / ".env"

# Banco obrigatório em dev (execução local): evita carga acidental em outro banco.
DEV_DB_NAME = "analytics_dev"


class DbTarget(BaseModel):
    """Perfil de conexão Postgres (um por ambiente).

    Os campos são opcionais para que um perfil inativo possa ficar incompleto
    no .env; ``Settings`` valida apenas o perfil do ``ENV`` ativo.
    """

    host: str | None = None
    port: int = 5432
    name: str | None = None
    user: str | None = None
    password: str | None = None

    def missing_fields(self) -> list[str]:
        return [f for f in ("host", "name", "user", "password") if not getattr(self, f)]

    @property
    def url(self) -> str:
        """URL SQLAlchemy; ``URL.create`` escapa senhas com caracteres especiais."""
        return URL.create(
            "postgresql+psycopg2",
            username=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.name,
        ).render_as_string(hide_password=False)


class DbProfiles(BaseModel):
    """Um ``DbTarget`` por ambiente (``DB__DEV__*``, ``DB__PROD__*``)."""

    dev: DbTarget = DbTarget()
    prod: DbTarget = DbTarget()


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
        # DB__DEV__HOST -> db.dev.host
        env_nested_delimiter="__",
        # Chave em branco no .env (ex.: DB__PROD__HOST=) conta como ausente.
        env_ignore_empty=True,
    )

    # dev = execução local; prod = Airflow
    env: Literal["dev", "prod"] = "dev"

    # Data lake local e seeds do dbt (repo externo my_analytics)
    lake_root: Path
    seeds_root: Path

    # Postgres (destino raw_<fonte>.*), um perfil por ambiente
    db: DbProfiles = DbProfiles()

    # Pipeline energia/solar
    apsystems_user: str | None = None
    apsystems_password: str | None = None
    apsystems_equipment_id: str | None = None

    # Pipeline financas/investimentos (google): service account e URLs das
    # planilhas, uma por chave do YAML (URL_FINANCE__<CHAVE>=...)
    google_credentials_file: Path | None = None
    url_finance: dict[str, str] = {}

    # Pipeline clima/openweather — chave da API One Call 3.0 e ponto consultado
    openweather_api_key: str | None = None
    openweather_lat: float | None = None
    openweather_lon: float | None = None

    # Selenium remoto (container selenium/standalone-chrome). Sem valor, os
    # pipelines com navegador (solar, fundos_imobiliarios) abrem Chrome local.
    selenium_remote_url: str | None = None

    @property
    def db_target(self) -> DbTarget:
        """Perfil de conexão do ambiente ativo."""
        return getattr(self.db, self.env)

    @property
    def db_url(self) -> str:
        return self.db_target.url

    @model_validator(mode="after")
    def _validate_db_target(self) -> "Settings":
        target = self.db_target
        if missing := target.missing_fields():
            prefix = f"DB__{self.env.upper()}__"
            faltam = ", ".join(prefix + m.upper() for m in missing)
            raise ValueError(f"ENV={self.env}: faltam {faltam} no .env")
        if self.env == "dev" and target.name != DEV_DB_NAME:
            raise ValueError(
                f"ENV=dev exige DB__DEV__NAME={DEV_DB_NAME}; recebido {target.name!r}"
            )
        return self


settings = Settings()
