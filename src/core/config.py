"""Configuração de pipelines dirigida por YAML.

Os YAMLs de fonte seguem a estrutura::

    db_schema: raw_<fonte>      # schema destino de todas as tabelas do arquivo
    load: table                 # modo de carga padrão (table | files | jsonb | none)
    bronze_sep: ";"             # separador do bronze (default ";")
    options: {...}              # opções comuns a todos os sources (opcional)
    environments:
      local:
        base_raw: ${LAKE_ROOT}/raw/...
        base_bronze: ${LAKE_ROOT}/bronze/...
        base_parameters: ${LAKE_ROOT}/raw/.../parameters
      airflow:
        ...
    sources:
      <entidade>:
        base_url: ...
        db_table: ...
        options: {...}          # sobrescreve as opções do topo

``db_schema``, ``load``, ``bronze_sep`` e ``options`` aceitam valor no topo do
arquivo (default) e por source (override). Placeholders ``${VAR}`` nos paths são
resolvidos contra o ambiente e o ``settings`` (LAKE_ROOT, SEEDS_ROOT).
"""

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from string import Template
from typing import Literal

import yaml

logger = logging.getLogger(__name__)

LoadMode = Literal["table", "files", "jsonb", "none"]
LOAD_MODES: tuple[str, ...] = ("table", "files", "jsonb", "none")


def _template_vars() -> dict[str, str]:
    values = dict(os.environ)
    try:
        from settings import settings

        values.setdefault("LAKE_ROOT", str(settings.lake_root))
        values.setdefault("SEEDS_ROOT", str(settings.seeds_root))
    except Exception:
        pass
    return values


def _expand_path(value: str) -> str:
    """Resolve ``${VAR}`` e ``~`` num path vindo de YAML."""
    return os.path.expanduser(Template(value).safe_substitute(_template_vars()))


def load_yaml(path: Path | str) -> dict:
    """Única implementação de leitura de YAML do monorepo."""
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


@dataclass
class PipelineConfig:
    """Contrato de configuração de um pipeline (um source do YAML).

    Args:
        landing_dir: diretório do dado bruto (obrigatório)
        bronze_dir: diretório do dado pós-transformação (CSV)
        parameter_dir: diretório dos CSVs de parâmetros (entrada e saída)
        url_base: URL da fonte
        subpath: subpasta aplicada a landing/bronze
        landing_file / bronze_file: nomes de arquivo; aceitam ``{date}``
        parameter_file: CSV de entrada que parametriza a extração
        output_param_file: str ou ``{arquivo: coluna}`` gerado para o próximo pipeline
        db_table: tabela destino (só a entidade)
        db_schema: schema destino, obrigatoriamente ``raw_<fonte>``
        load: modo de carga (``table`` | ``files`` | ``jsonb`` | ``none``)
        bronze_sep: separador do CSV bronze
        options: dict livre com o bloco ``options:`` do YAML
        criar_dirs: cria os diretórios no ``__init__`` (em testes use ``False``)
    """

    landing_dir: Path | str
    bronze_dir: Path | str | None = None
    parameter_dir: Path | str | None = None
    url_base: str | None = None
    subpath: str | None = None
    landing_file: str | None = None
    bronze_file: str | None = None
    parameter_file: str | None = None
    output_param_file: str | dict[str, str] | None = None
    db_table: str | None = None
    db_schema: str | None = None
    load: LoadMode = "table"
    bronze_sep: str = ";"
    options: dict = field(default_factory=dict)
    criar_dirs: bool = True

    def __post_init__(self):
        if self.load not in LOAD_MODES:
            raise ValueError(f"load={self.load!r} inválido; use um de {LOAD_MODES}.")

        self.landing_dir = self._to_path(self.landing_dir, self.subpath)
        if self.bronze_dir:
            self.bronze_dir = self._to_path(self.bronze_dir, self.subpath)
        if self.parameter_dir:
            self.parameter_dir = Path(_expand_path(str(self.parameter_dir)))

        # {date} é o único placeholder que a core resolve; outros ({game_id},
        # {day}) são preservados para o pipeline resolver com str.format.
        today = datetime.today().strftime("%Y-%m-%d")
        if self.landing_file:
            self.landing_file = self.landing_file.replace("{date}", today)
        if self.bronze_file:
            self.bronze_file = self.bronze_file.replace("{date}", today)
        if self.bronze_file is None and self.landing_file:
            self.bronze_file = Path(self.landing_file).with_suffix(".csv").name

        if self.criar_dirs:
            for d in (self.landing_dir, self.bronze_dir, self.parameter_dir):
                if d is not None:
                    Path(d).mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _to_path(base: Path | str, subpath: str | None) -> Path:
        path = Path(_expand_path(str(base)))
        return path / subpath if subpath else path

    @classmethod
    def from_yaml(
        cls,
        config_file: Path | str,
        source: str,
        *,
        env: str | None = None,
        **overrides,
    ) -> "PipelineConfig":
        """Monta a config do ``source`` a partir do YAML da fonte.

        ``env`` default vem de ``settings.env`` (variável ENV do .env da raiz);
        ``overrides`` sobrescrevem qualquer campo (ex.: ``criar_dirs=False``).
        """
        return cls(**{**_source_dict(config_file, source, env), **overrides})

    @property
    def landing_filepath(self) -> Path:
        if not self.landing_file:
            raise ValueError("landing_file não configurado na PipelineConfig.")
        return self.landing_dir / self.landing_file

    @property
    def bronze_filepath(self) -> Path:
        if not self.bronze_dir:
            raise ValueError("bronze_dir não configurado na PipelineConfig.")
        if not self.bronze_file:
            raise ValueError("bronze_file não configurado na PipelineConfig.")
        return Path(self.bronze_dir) / self.bronze_file

    @property
    def parameter_filepath(self) -> Path:
        if not self.parameter_dir:
            raise ValueError("parameter_dir não configurado na PipelineConfig.")
        if not self.parameter_file:
            raise ValueError("parameter_file não configurado na PipelineConfig.")
        return Path(self.parameter_dir) / self.parameter_file

    def write_output_params(self, df, default_column: str | None = None) -> None:
        """Exporta CSV(s) de parâmetros em ``parameter_dir`` a partir de ``df``.

        ``output_param_file`` pode ser ``str`` (exporta ``default_column``) ou
        ``dict {arquivo: coluna}``. Cada saída recebe os valores únicos da coluna;
        coluna ausente é pulada com warning.
        """
        if not self.output_param_file:
            return
        if not self.parameter_dir:
            raise ValueError("parameter_dir não configurado na PipelineConfig.")

        if isinstance(self.output_param_file, dict):
            exports = dict(self.output_param_file)
        else:
            if default_column is None:
                raise ValueError(
                    "output_param_file é str; informe default_column para saber "
                    "qual coluna exportar."
                )
            exports = {self.output_param_file: default_column}

        Path(self.parameter_dir).mkdir(parents=True, exist_ok=True)
        for fname, col in exports.items():
            path = Path(self.parameter_dir) / fname
            if col not in df.columns:
                logger.warning(
                    f"⚠️ Coluna '{col}' não encontrada - {fname} não exportado."
                )
                continue
            df[[col]].dropna().drop_duplicates().to_csv(path, index=False)
            logger.info(f"📄 IDs exportados para: {path}")


def _source_dict(config_file: Path | str, source: str, env: str | None) -> dict:
    """Dict compatível com ``PipelineConfig`` para um source do YAML."""
    if env is None:
        try:
            from settings import settings

            env = settings.env
        except Exception:
            env = os.getenv("ENV", "local")

    cfg = load_yaml(config_file)
    env_cfg = cfg["environments"][env]
    src_cfg = cfg["sources"][source]

    result: dict = {
        "landing_dir": env_cfg["base_raw"],
        "bronze_dir": env_cfg.get("base_bronze"),
        "parameter_dir": env_cfg.get("base_parameters"),
        "url_base": src_cfg.get("base_url"),
        "subpath": src_cfg.get("subpath"),
        "landing_file": src_cfg.get("landing_file"),
        "bronze_file": src_cfg.get("bronze_file"),
        "parameter_file": src_cfg.get("parameter_file"),
        "output_param_file": src_cfg.get("output_param_file"),
        "db_table": src_cfg.get("db_table"),
        # Valor do topo é o default do arquivo; o source pode sobrescrever.
        "db_schema": src_cfg.get("db_schema", cfg.get("db_schema")),
        "load": src_cfg.get("load", cfg.get("load")),
        "bronze_sep": src_cfg.get("bronze_sep", cfg.get("bronze_sep")),
        "options": {**(cfg.get("options") or {}), **(src_cfg.get("options") or {})},
    }
    return {k: v for k, v in result.items() if v is not None}
