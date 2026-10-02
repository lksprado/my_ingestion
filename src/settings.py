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
Guard-rail: em ``ENV=dev`` o banco tem que ser ``ingestion_sandbox``. Perfis
inativos podem ficar em branco no .env.

Em dev, cargas e objetos do dbt vivem em bancos diferentes: a carga vai para o
sandbox, e o que o dbt constrói (``intermediate``, marts) fica no
``analytics_dev``. Leituras desses objetos usam ``settings.models_target`` — hoje
só o ``investimentos_fgc``. Em prod os dois são o mesmo banco.

Configuração de fonte
---------------------
Aqui fica só o que é da plataforma ou serve a mais de uma fonte (ambiente, lake,
banco, Selenium). Credencial de uma fonte é declarada no ``<fonte>_etl.py`` numa
subclasse de ``SourceSettings`` com o prefixo da fonte e carregada no extract com
``.carregar()``: os campos podem ser obrigatórios sem quebrar a importação das
outras fontes, e a falta aparece só no pipeline que precisa deles.
"""

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ValidationError, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL

# Ancorado na raiz do repo (src/settings.py -> ../), para que os pipelines
# rodem de qualquer diretório, não só da raiz.
REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = REPO_ROOT / ".env"

# Banco obrigatório em dev (execução local): testes de carga não sujam a raw que o
# dbt consome no analytics_dev.
DEV_DB_NAME = "ingestion_sandbox"
DEV_MODELS_DB_NAME = "analytics_dev"


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


# Comum ao Settings e às SourceSettings das fontes.
_MODEL_CONFIG = SettingsConfigDict(
    env_file=ENV_FILE,
    env_file_encoding="utf-8",
    extra="ignore",
    # DB__DEV__HOST -> db.dev.host; URL_FINANCE__X -> url_finance["x"]
    env_nested_delimiter="__",
    # Chave em branco no .env (ex.: DB__PROD__HOST=) conta como ausente.
    env_ignore_empty=True,
)


class SourceSettings(BaseSettings):
    """Base da configuração de uma fonte, lida do mesmo .env (ou do ambiente).

    A subclasse fica no ``<fonte>_etl.py``, só acrescenta o prefixo e declara os
    campos (obrigatórios sem default)::

        class OpenweatherSettings(SourceSettings):
            model_config = SettingsConfigDict(env_prefix="OPENWEATHER_")
            api_key: str

        cfg = OpenweatherSettings.carregar()  # no extract, não no import
    """

    model_config = _MODEL_CONFIG

    @classmethod
    def carregar(cls, **valores):
        """Instancia; variável faltando vira ``ValueError`` com o nome dela."""
        try:
            return cls(**valores)
        except ValidationError as e:
            prefixo = cls.model_config.get("env_prefix", "")
            nomes = sorted(
                {
                    f"{prefixo}{err['loc'][0]}".upper()
                    for err in e.errors()
                    if err["loc"]
                }
            )
            raise ValueError(
                f"{cls.__name__}: ausente(s) ou inválida(s) no .env: {', '.join(nomes)}"
            ) from None


class Settings(BaseSettings):
    model_config = _MODEL_CONFIG

    # dev = execução local; prod = Airflow
    env: Literal["dev", "prod"] = "dev"

    # Data lake local e seeds do dbt (repo externo my_analytics)
    lake_root: Path
    seeds_root: Path

    # Postgres (destino raw_<fonte>.*), um perfil por ambiente
    db: DbProfiles = DbProfiles()

    # Selenium remoto (container selenium/standalone-chrome). Sem valor, os
    # pipelines com navegador (solar, fundos_imobiliarios) abrem Chrome local.
    selenium_remote_url: str | None = None

    @property
    def db_target(self) -> DbTarget:
        """Perfil de conexão do ambiente ativo."""
        return getattr(self.db, self.env)

    @property
    def models_target(self) -> DbTarget:
        """Banco com os objetos do dbt; em dev, o perfil dev no analytics_dev."""
        if self.env == "dev":
            return self.db.dev.model_copy(update={"name": DEV_MODELS_DB_NAME})
        return self.db_target

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
