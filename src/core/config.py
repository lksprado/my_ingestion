"""Configuração de pipelines dirigida por YAML (ex-pipeline_cfg.py de demodados).

Os YAMLs de fonte seguem a estrutura::

    environments:
      local:
        base_raw: ${LAKE_ROOT}/raw/...
        base_bronze: ${LAKE_ROOT}/bronze/...
      airflow:
        ...
    sources:
      <nome>:
        base_url: ...
        db_table: ...

Placeholders ``${VAR}`` nos paths são resolvidos contra o ambiente e o
``settings`` (LAKE_ROOT, SEEDS_ROOT), eliminando paths absolutos.
"""

import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from string import Template

import yaml


def _template_vars() -> dict[str, str]:
    values = dict(os.environ)
    try:
        from settings import settings

        values.setdefault("LAKE_ROOT", str(settings.lake_root))
        values.setdefault("SEEDS_ROOT", str(settings.seeds_root))
    except Exception:
        pass
    return values


def expand_path(value: str) -> str:
    """Resolve ``${VAR}`` e ``~`` num path vindo de YAML."""
    return os.path.expanduser(Template(value).safe_substitute(_template_vars()))


def load_yaml(path: Path | str) -> dict:
    """Única implementação de leitura de YAML do monorepo."""
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


@dataclass
class PipelineConfig:
    """Contrato para configuração do pipeline.
    Forneça um dicionário contendo:
    Args:
        landing_dir: diretorio arquivos bruto
        bronze_dir: diretorio pos transformacao
        error_dir: diretorio fallback se houver
        parameter_file: arquivo para parametrizar
        db_table: nome tabela banco de dados
    """

    landing_dir: Path | str
    bronze_dir: Path | str | None = None
    db_table: str | None = None
    url_base: str | None = None
    subpath: str = None
    error_dir: Path | str = None
    landing_file: str | None = None
    bronze_file: str | None = None
    parameter_dir: Path | str | None = None
    parameter_file: str | None = None
    output_param_dir: Path | str | None = None
    # str: uma única saída (coluna definida pelo pipeline)
    # dict {arquivo: coluna}: múltiplas saídas, cada uma com sua coluna
    output_param_file: str | dict[str, str] | None = None
    # Chaves específicas da fonte que a core não interpreta (bloco ``options:``
    # do YAML): array_key, overwrite, lat/lon, workers...
    options: dict = field(default_factory=dict)
    criar_dirs: bool = True

    def __post_init__(self):
        # Normaliza diretórios para Path, resolvendo ${VAR} vindos do YAML
        self.landing_dir = self._to_path(self.landing_dir, self.subpath)
        if self.bronze_dir:
            self.bronze_dir = self._to_path(self.bronze_dir, self.subpath)
        if self.error_dir:
            self.error_dir = self._to_path(self.error_dir, self.subpath)
        if self.parameter_dir:
            self.parameter_dir = Path(expand_path(str(self.parameter_dir)))
        if self.output_param_dir:
            self.output_param_dir = Path(expand_path(str(self.output_param_dir)))

        # Resolve o template {date} em landing_file e bronze_file. É o único
        # placeholder que a core conhece; outros (ex.: {game_id}) são preservados
        # para o pipeline resolver com str.format na hora da extração.
        today = datetime.today().strftime("%Y-%m-%d")
        if self.landing_file:
            self.landing_file = self.landing_file.replace("{date}", today)
        if self.bronze_file:
            self.bronze_file = self.bronze_file.replace("{date}", today)

        # Deriva bronze_file se não vier no config
        if self.bronze_file is None and self.landing_file:
            self.bronze_file = Path(self.landing_file).with_suffix(".csv").name

        if self.criar_dirs:
            self.ensure_dirs()

    @staticmethod
    def _to_path(base: Path | str, subpath: str | None) -> Path:
        path = Path(expand_path(str(base)))
        return path / subpath if subpath else path

    def ensure_dirs(self) -> None:
        self.landing_dir.mkdir(parents=True, exist_ok=True)
        if self.bronze_dir is not None:
            self.bronze_dir.mkdir(parents=True, exist_ok=True)
        if self.error_dir is not None:
            self.error_dir.mkdir(parents=True, exist_ok=True)
        if self.output_param_dir is not None:
            self.output_param_dir.mkdir(parents=True, exist_ok=True)

    @property
    def landing_filepath(self) -> Path:
        if not self.landing_dir:
            raise ValueError("landing_dir não configurado na PipelineConfig.")
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

    @property
    def output_param_filepath(self) -> Path:
        if not self.output_param_dir:
            raise ValueError("output_param_dir não configurado na PipelineConfig.")
        if not self.output_param_file:
            raise ValueError("output_param_file não configurado na PipelineConfig.")
        if isinstance(self.output_param_file, dict):
            raise ValueError(
                "output_param_file é um mapeamento múltiplo (dict); "
                "use write_output_params() em vez de output_param_filepath."
            )
        return Path(self.output_param_dir) / self.output_param_file

    def write_output_params(
        self, df, default_column: str | None = None, logger=None
    ) -> None:
        """Exporta arquivo(s) de parâmetros de saída a partir de ``df``.

        ``output_param_file`` pode ser:
        - ``str``: exporta ``default_column`` (obrigatório) para esse arquivo.
        - ``dict {arquivo: coluna}``: exporta cada coluna para o arquivo correspondente.

        Cada saída recebe os valores únicos (``drop_duplicates``) da coluna.
        Colunas ausentes no DataFrame são puladas com um warning.
        """
        if not self.output_param_file:
            return
        if not self.output_param_dir:
            raise ValueError("output_param_dir não configurado na PipelineConfig.")

        if isinstance(self.output_param_file, dict):
            exports = dict(self.output_param_file)
        else:
            if default_column is None:
                raise ValueError(
                    "output_param_file é str; informe default_column para saber "
                    "qual coluna exportar."
                )
            exports = {self.output_param_file: default_column}

        for fname, col in exports.items():
            path = Path(self.output_param_dir) / fname
            if col not in df.columns:
                if logger:
                    logger.warning(
                        f"⚠️ Coluna '{col}' não encontrada - {fname} não exportado."
                    )
                continue
            df[[col]].dropna().drop_duplicates().to_csv(path, index=False)
            if logger:
                logger.info(f"📄 IDs exportados para: {path}")


def load_source_config(config_path: str, source: str, env: str | None = None) -> dict:
    """Monta dict compatível com PipelineConfig a partir de YAML com sections
    environments/sources.

    ``env`` default vem de ``settings.env`` (variável ENV do .env da raiz).
    """
    if env is None:
        try:
            from settings import settings

            env = settings.env
        except Exception:
            env = os.getenv("ENV", "local")

    cfg = load_yaml(config_path)
    env_cfg = cfg["environments"][env]
    src_cfg = cfg["sources"][source]

    result: dict = {
        "landing_dir": env_cfg["base_raw"],
        "bronze_dir": env_cfg.get("base_bronze"),
        "parameter_dir": env_cfg.get("base_parameters"),
        "url_base": src_cfg.get("base_url"),
        "bronze_file": src_cfg.get("bronze_file"),
        "landing_file": src_cfg.get("landing_file"),
        "subpath": src_cfg.get("subpath"),
        "db_table": src_cfg.get("db_table"),
        "parameter_file": src_cfg.get("parameter_file"),
        "output_param_dir": env_cfg.get("base_parameters"),
        "output_param_file": src_cfg.get("output_param_file"),
        "options": src_cfg.get("options"),
    }

    return {k: v for k, v in result.items() if v is not None}
