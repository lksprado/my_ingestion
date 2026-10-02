"""Helpers de arquivo: listagem, concat e escrita de CSV/bronze.

``write_bronze`` e ``write_bronze_streaming`` são a forma única de terminar um
``transform``: sanitizam colunas, removem quebras de linha, gravam inteiros sem
``.0`` e escrevem ``cfg.bronze_filepath`` com ``cfg.bronze_sep``.
"""

import csv
import logging
import multiprocessing
import os
import sys
import tempfile
import traceback
from collections import deque
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import ProcessPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field
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
    input_dir: Path | str, pattern: str = "*.csv", sep: str = ","
) -> pd.DataFrame:
    """Concatena os CSVs de um diretório num único DataFrame.

    Diretório sem arquivos devolve DataFrame vazio (com warning).
    """
    input_dir = Path(input_dir)
    files = sorted(input_dir.glob(pattern))
    if not files:
        logger.warning(f"⚠️ Nenhum arquivo ({pattern}) em {input_dir}")
        return pd.DataFrame()
    return pd.concat(
        (pd.read_csv(f, sep=sep, encoding="utf-8", low_memory=False) for f in files),
        ignore_index=True,
    )


def concat_landing(
    cfg: PipelineConfig,
    parse_fn: Callable[[Path], pd.DataFrame | None],
    pattern: str = "*.json",
    landing_dir: Path | None = None,
) -> pd.DataFrame:
    """Aplica ``parse_fn`` a cada arquivo do landing e concatena.

    Exceção num arquivo é logada e o arquivo pulado; ``None``/vazio é ignorado.
    Sem nenhum dado, devolve DataFrame vazio. ``landing_dir`` lê o landing de
    outra entidade (default: ``cfg.landing_dir``).
    """
    frames = []
    for f in sorted((landing_dir or cfg.landing_dir).glob(pattern)):
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


ParseFn = Callable[[Path], pd.DataFrame | None]


def _parse_prepared(
    parse_fn: ParseFn, file: Path
) -> tuple[pd.DataFrame | None, str | None]:
    """``(df preparado ou None, traceback ou None)`` de um arquivo.

    Fica no nível do módulo para ir aos processos do pool; a exceção volta como
    texto para o processo principal logar e seguir.
    """
    try:
        df = parse_fn(file)
    except Exception:
        return None, traceback.format_exc()
    if df is None or df.empty:
        return None, None
    return _prepare(df), None


@contextmanager
def _main_fora_dos_filhos(parse_fn: ParseFn) -> Iterator[None]:
    """Impede o pool de reexecutar o ``__main__`` quando ``parse_fn`` não mora nele.

    ``forkserver`` (como ``spawn``) importa de novo o script principal em cada
    processo novo. Numa task do Airflow esse script é o ``airflow``, cujo import
    tenta abrir o banco de metadados, bloqueado ali (``airflow-db-not-allowed``),
    e o filho morre. O filho só precisa do módulo de ``parse_fn``; quando ele é o
    próprio ``__main__`` (``python -m pipelines...``), nada muda.
    """
    main = sys.modules["__main__"]
    modulo = getattr(getattr(parse_fn, "func", parse_fn), "__module__", None)
    if modulo == "__main__":
        yield
        return
    sem = object()
    originais = {a: getattr(main, a, sem) for a in ("__file__", "__spec__")}
    main.__spec__ = None
    if originais["__file__"] is not sem:
        del main.__file__
    try:
        yield
    finally:
        for attr, valor in originais.items():
            if valor is not sem:
                setattr(main, attr, valor)


def _prepared_frames(
    files: Iterable[Path], parse_fn: ParseFn, workers: int
) -> Iterator[tuple[Path, pd.DataFrame | None, str | None]]:
    """``_parse_prepared`` de cada arquivo, na ordem de ``files``.

    Com ``workers > 1`` os arquivos são processados num pool de processos, com
    no máximo ``2 * workers`` em andamento: a escrita é sequencial, e sem esse
    limite os DataFrames prontos se acumulariam na memória à espera dela. O pool
    usa ``forkserver``: ``fork`` de um processo com threads (as do pyarrow, por
    exemplo) pode travar o filho.
    """
    if workers <= 1:
        for file in files:
            yield file, *_parse_prepared(parse_fn, file)
        return
    contexto = multiprocessing.get_context("forkserver")
    with (
        _main_fora_dos_filhos(parse_fn),
        ProcessPoolExecutor(workers, mp_context=contexto) as pool,
    ):
        pendentes = deque()
        for file in files:
            pendentes.append((file, pool.submit(_parse_prepared, parse_fn, file)))
            if len(pendentes) >= 2 * workers:
                file_, fut = pendentes.popleft()
                yield file_, *fut.result()
        while pendentes:
            file_, fut = pendentes.popleft()
            yield file_, *fut.result()


@dataclass
class StreamResult:
    """O que ``stream_bronze`` fez: o bronze gravado e o destino de cada arquivo."""

    path: Path | None  # None: nenhum arquivo com dados, bronze anterior intacto
    parsed: list[Path] = field(default_factory=list)  # lidos sem erro (com ou sem dado)
    failed: list[Path] = field(default_factory=list)  # parse_fn levantou exceção


def _pad_rows(src: str, dst: str, header: list[str], sep: str) -> None:
    """Regrava ``src`` em ``dst`` com ``header``, completando as linhas curtas.

    Coluna nova sempre entra no fim do cabeçalho, então a linha escrita antes
    dela só precisa de campos vazios (NULL no COPY) à direita. O ``csv`` é o
    mesmo módulo que o pandas usa para escrever, então a citação não muda.
    """
    # O leitor limita o campo a 128 KB; o pandas escreve campo de qualquer tamanho.
    csv.field_size_limit(2**31 - 1)
    with (
        open(src, encoding="utf-8", newline="") as fin,
        open(dst, "w", encoding="utf-8", newline="") as fout,
    ):
        reader = csv.reader(fin, delimiter=sep)
        writer = csv.writer(fout, delimiter=sep, lineterminator="\n")
        next(reader)  # cabeçalho antigo
        writer.writerow(header)
        for row in reader:
            writer.writerow(row + [""] * (len(header) - len(row)))


def stream_bronze(
    cfg: PipelineConfig,
    files: Iterable[Path],
    parse_fn: ParseFn,
    workers: int | None = None,
) -> StreamResult:
    """Implementação do ``write_bronze_streaming``, com o destino de cada arquivo.

    ``write_bronze_incremental`` precisa saber quais arquivos deram erro, para
    não registrá-los como carregados.
    """
    if workers is None:
        workers = int(cfg.options.get("transform_workers", 1))
    bronze_path = cfg.bronze_filepath
    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    result = StreamResult(path=None)
    header: list[str] | None = None
    written_cols = 0  # tamanho do cabeçalho gravado na 1ª linha do temporário
    n_files = n_rows = 0

    tmp_names = []
    for _ in range(2):
        fd, name = tempfile.mkstemp(
            dir=bronze_path.parent, prefix=f".{bronze_path.stem}_", suffix=".tmp"
        )
        os.close(fd)
        tmp_names.append(name)
    tmp_name, padded_name = tmp_names
    try:
        with open(tmp_name, "w", encoding="utf-8", newline="") as out:
            for file, df, erro in _prepared_frames(files, parse_fn, workers):
                if erro:
                    logger.error(f"❌ Erro ao transformar {file}\n{erro}")
                    result.failed.append(file)
                    continue
                result.parsed.append(file)
                if df is None:
                    continue
                if header is None:
                    header = list(df.columns)
                    written_cols = len(header)
                    df.to_csv(out, sep=cfg.bronze_sep, index=False, header=True)
                else:
                    novas = [c for c in df.columns if c not in header]
                    if novas:
                        logger.info(f"➕ {file.name}: colunas novas no bronze {novas}")
                        header.extend(novas)
                    df = df.reindex(columns=header)
                    df.to_csv(out, sep=cfg.bronze_sep, index=False, header=False)
                n_files += 1
                n_rows += len(df)

        if header is None:
            logger.warning("⚠️ Nenhum arquivo com dados; bronze anterior preservado.")
            return result

        if len(header) > written_cols:
            _pad_rows(tmp_name, padded_name, header, cfg.bronze_sep)
            tmp_name, padded_name = padded_name, tmp_name
        os.chmod(tmp_name, 0o664)
        os.replace(tmp_name, bronze_path)
        logger.info(
            f"💾 Bronze: {n_files} arquivo(s), {n_rows} linha(s) em {bronze_path}"
        )
        result.path = bronze_path
        return result
    finally:
        for name in tmp_names:
            if os.path.exists(name):
                os.unlink(name)


def write_bronze_streaming(
    cfg: PipelineConfig,
    files: Iterable[Path],
    parse_fn: ParseFn,
    workers: int | None = None,
) -> Path | None:
    """Reconstrói ``cfg.bronze_filepath`` um arquivo por vez (memória limitada).

    ``parse_fn(file)`` devolve o DataFrame daquele arquivo (ou ``None`` para
    pular). O cabeçalho começa com as colunas do primeiro DataFrame e cresce
    quando um arquivo seguinte traz coluna nova: ela entra no fim e fica vazia
    (NULL) nas linhas anteriores — o arquivo só é regravado nesse caso. Nenhuma
    coluna é descartada. Exceção num arquivo é logada e o arquivo pulado. A
    escrita vai para um temporário e substitui o bronze atomicamente; sem nenhum
    dado, o bronze anterior é preservado e a função devolve ``None``.

    ``workers`` (default: ``options.transform_workers`` do YAML, senão 1) > 1
    faz o parse em paralelo num pool de processos; a saída é a mesma, na mesma
    ordem. Aí ``parse_fn`` tem de ser picklável: função de módulo ou
    ``functools.partial`` dela, não lambda.
    """
    return stream_bronze(cfg, files, parse_fn, workers).path
