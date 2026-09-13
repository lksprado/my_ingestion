"""Helpers de arquivo: listagem, concat e escrita de CSV/bronze.

``write_bronze`` e ``write_bronze_streaming`` são a forma única de terminar um
``transform``: sanitizam colunas, removem quebras de linha, gravam inteiros sem
``.0`` e escrevem ``cfg.bronze_filepath`` com ``cfg.bronze_sep``.
"""

import logging
import os
import tempfile
from collections.abc import Callable, Iterable
from pathlib import Path

import pandas as pd

from core.config import PipelineConfig
from core.text import sanitize_columns, strip_newlines

logger = logging.getLogger(__name__)


def list_files(input_dir: Path | str, pattern: str = "*") -> list[Path]:
    """Lista arquivos recursivamente, ordenados e resolvidos."""
    input_dir = Path(input_dir)
    return sorted(f.resolve() for f in input_dir.rglob(pattern) if f.is_file())


def concat_files_to_df(
    input_dir: Path | str,
    pattern: str = "*.csv",
    sep: str = ",",
    source_column: str | None = None,
) -> pd.DataFrame:
    """Concatena CSV/JSON de um diretório num único DataFrame.

    ``source_column`` adiciona uma coluna com o nome do arquivo de origem.
    Diretório sem arquivos devolve DataFrame vazio (com warning).
    """
    input_dir = Path(input_dir)
    files = sorted(input_dir.glob(pattern))
    if not files:
        logger.warning(f"⚠️ Nenhum arquivo ({pattern}) em {input_dir}")
        return pd.DataFrame()

    dfs = []
    for file in files:
        if file.suffix.lower() == ".json":
            df = pd.read_json(file, encoding="utf-8")
        else:
            df = pd.read_csv(file, sep=sep, encoding="utf-8", low_memory=False)
        if source_column:
            df[source_column] = file.name
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)


def concat_landing(
    cfg: PipelineConfig,
    parse_fn: Callable[[Path], pd.DataFrame | None],
    pattern: str = "*.json",
) -> pd.DataFrame:
    """Aplica ``parse_fn`` a cada arquivo do landing e concatena.

    Exceção num arquivo é logada e o arquivo pulado; ``None``/vazio é ignorado.
    Sem nenhum dado, devolve DataFrame vazio.
    """
    frames = []
    for f in sorted(cfg.landing_dir.glob(pattern)):
        try:
            df = parse_fn(f)
        except Exception:
            logger.error(f"❌ Erro ao transformar {f}", exc_info=True)
            continue
        if df is not None and not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def write_csv(
    df: pd.DataFrame, output_dir: Path | str, filename: str, sep: str = ";"
) -> Path:
    """Grava um DataFrame em CSV, criando o diretório se preciso."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    if not filename.endswith(".csv"):
        filename = f"{filename}.csv"
    filepath = output_dir / filename
    df.to_csv(filepath, sep=sep, index=False)
    logger.info(f"💾 CSV salvo em: {filepath}")
    return filepath


# Acima disso o float já não representa o inteiro exatamente.
_MAX_SAFE_INT = 2**53


def integral_floats_to_int(df: pd.DataFrame) -> pd.DataFrame:
    """Colunas float cujos valores não nulos são todos inteiros viram ``Int64``.

    O pandas promove para float a coluna inteira que tem nulo (``json_normalize``,
    ``concat``), e o CSV sairia ``123.0``. Como a raw é texto, isso quebraria
    ``'123.0'::int`` no dbt; com ``Int64`` o bronze grava ``123``.
    """
    df = df.copy()
    for col in df.select_dtypes(include="float").columns:
        present = df[col].dropna()
        if (
            len(present)
            and (present % 1 == 0).all()
            and (present.abs() < _MAX_SAFE_INT).all()
        ):
            df[col] = df[col].astype("Int64")
    return df


def _prepare(df: pd.DataFrame) -> pd.DataFrame:
    return integral_floats_to_int(strip_newlines(sanitize_columns(df)))


def reset_bronze(cfg: PipelineConfig) -> None:
    """Apaga os CSVs de ``cfg.bronze_dir`` antes de regravar.

    Para ``load: files``, em que cada arquivo do diretório vira uma tabela: o
    bronze é inteiramente derivado do landing (full refresh), então um CSV
    antigo que sobrasse viraria uma tabela fantasma.
    """
    old = list(cfg.bronze_dir.glob("*.csv"))
    for f in old:
        f.unlink()
    if old:
        logger.info(f"🧹 {len(old)} CSV(s) antigo(s) removido(s) de {cfg.bronze_dir}")


def write_bronze(cfg: PipelineConfig, df: pd.DataFrame | None) -> Path | None:
    """Grava ``df`` em ``cfg.bronze_filepath`` (colunas sanitizadas, sem CR/LF).

    DataFrame vazio ou ``None``: warning, não grava, devolve ``None`` — o bronze
    anterior fica intacto.
    """
    if df is None or df.empty:
        logger.warning("⚠️ Nada a gravar no bronze; arquivo anterior preservado.")
        return None
    path = cfg.bronze_filepath
    path.parent.mkdir(parents=True, exist_ok=True)
    df = _prepare(df)
    df.to_csv(path, sep=cfg.bronze_sep, index=False)
    logger.info(f"💾 Bronze: {len(df)} linha(s) em {path}")
    return path


def write_bronze_streaming(
    cfg: PipelineConfig,
    files: Iterable[Path],
    parse_fn: Callable[[Path], pd.DataFrame | None],
) -> Path | None:
    """Reconstrói ``cfg.bronze_filepath`` um arquivo por vez (memória limitada).

    ``parse_fn(file)`` devolve o DataFrame daquele arquivo (ou ``None`` para
    pular). O cabeçalho é fixado pelo primeiro DataFrame; os demais são
    alinhados a ele (colunas extras são descartadas com warning). Exceção num
    arquivo é logada e o arquivo pulado. A escrita vai para um temporário e
    substitui o bronze atomicamente; sem nenhum dado, o bronze anterior é
    preservado e a função devolve ``None``.
    """
    bronze_path = cfg.bronze_filepath
    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    header: list[str] | None = None
    n_files = n_rows = 0

    fd, tmp_name = tempfile.mkstemp(
        dir=bronze_path.parent, prefix=f".{bronze_path.stem}_", suffix=".tmp"
    )
    os.close(fd)
    try:
        with open(tmp_name, "w", encoding="utf-8", newline="") as out:
            for file in files:
                try:
                    df = parse_fn(file)
                except Exception:
                    logger.error(f"❌ Erro ao transformar {file}", exc_info=True)
                    continue
                if df is None or df.empty:
                    continue
                df = _prepare(df)
                if header is None:
                    header = list(df.columns)
                    df.to_csv(out, sep=cfg.bronze_sep, index=False, header=True)
                else:
                    extras = [c for c in df.columns if c not in header]
                    if extras:
                        logger.warning(
                            f"⚠️ {file.name}: colunas extras ignoradas {extras}"
                        )
                    df = df.reindex(columns=header)
                    df.to_csv(out, sep=cfg.bronze_sep, index=False, header=False)
                n_files += 1
                n_rows += len(df)

        if header is None:
            logger.warning("⚠️ Nenhum arquivo com dados; bronze anterior preservado.")
            return None

        os.chmod(tmp_name, 0o664)
        os.replace(tmp_name, bronze_path)
        logger.info(
            f"💾 Bronze: {n_files} arquivo(s), {n_rows} linha(s) em {bronze_path}"
        )
        return bronze_path
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)
