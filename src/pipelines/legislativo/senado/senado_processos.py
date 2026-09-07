import json
import logging
import os
import shutil
import tempfile
from pathlib import Path

import pandas as pd

from core import GenericETL, PipelineConfig, load_source_config
from core.http import HttpClient
from core.text import ColumnSanitizer

logger = logging.getLogger("raw_senado_processos")

_CONFIG_FILE = Path(__file__).parent / "senado_config.yml"
_SEM_DADOS_FILE = "sem_dados_id_processo.csv"
_LANDING_SUFFIX = "_processo"
# Coluna que guarda o idProcesso consultado (chave de idempotencia no bronze).
_ID_COL = "id_processo"


def extract(cfg: PipelineConfig):
    logger.info("📥 Iniciando extracao...")
    extractor = HttpClient(logger)

    # Seed gerado por senado_votacoes (coluna idprocesso). Normaliza para int -> str
    # para evitar ids com ".0" e descarta vazios/duplicados.
    raw_ids = pd.read_csv(cfg.parameter_filepath)["idprocesso"]
    todos_ids = pd.DataFrame(
        {
            "id": pd.to_numeric(raw_ids, errors="coerce")
            .dropna()
            .astype("int64")
            .astype(str)
            .unique()
        }
    )

    ids_landing = pd.DataFrame(
        {
            "id": [
                f.stem.removesuffix(_LANDING_SUFFIX)
                for f in cfg.landing_dir.iterdir()
                if f.suffix == ".json"
            ]
        }
    )

    sem_dados_path = cfg.parameter_dir / _SEM_DADOS_FILE
    ids_sem_dados = (
        pd.read_csv(sem_dados_path)[["id"]].astype(str)
        if sem_dados_path.exists()
        else pd.DataFrame(columns=["id"])
    )

    ja_processados = pd.concat(
        [ids_landing, ids_sem_dados], ignore_index=True
    ).drop_duplicates()

    pendentes = (
        todos_ids.merge(ja_processados, on="id", how="left", indicator=True)
        .query('_merge == "left_only"')["id"]
        .tolist()
    )

    logger.info(
        f"{len(pendentes)} processos pendentes "
        f"(total: {len(todos_ids)}, ja extraidos: {len(ids_landing)}, sem dados: {len(ids_sem_dados)})."
    )

    for proc_id in pendentes:
        url = f"{cfg.url_base}?idProcesso={proc_id}&v=1"
        data = extractor.make_http_request(url)
        if data:
            extractor.save_response(
                data, cfg.landing_dir, f"{proc_id}{_LANDING_SUFFIX}.json"
            )
        else:
            if data is None:
                logger.warning(
                    f"⚠️ Erro/timeout para processo {proc_id}, registrando para pular nas proximas runs."
                )
            else:
                logger.warning(
                    f"⚠️ Sem dados para processo {proc_id}, registrando para ignorar nas proximas runs."
                )
            write_header = not sem_dados_path.exists()
            with open(sem_dados_path, "a", encoding="utf-8", newline="") as f:
                if write_header:
                    f.write("id\n")
                f.write(f"{proc_id}\n")


def _id_from_filename(f: Path) -> str:
    """Id do processo a partir do nome do arquivo de landing."""
    return f.stem.removesuffix(_LANDING_SUFFIX)


def _parse_file(f: Path) -> pd.DataFrame | None:
    """Le um JSON de landing (array na raiz) e devolve o DataFrame ja sanitizado,
    com a coluna id_processo (id consultado), ou None se vazio."""
    with open(f, encoding="utf-8") as fp:
        raw = json.load(fp)

    if not raw:
        return None

    df = pd.json_normalize(raw, sep=".")
    if df.empty:
        return None

    df = ColumnSanitizer(df).sanitize_columns_names().df

    # Remove quebras de linha embutidas em colunas de texto (ex: ementa) para evitar
    # ParserError/buffer overflow no pd.read_csv posterior (load).
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(
        lambda col: col.str.replace(r"[\r\n]+", " ", regex=True)
    )

    # id consultado vem do nome do arquivo; nao depende de campo da resposta.
    df[_ID_COL] = _id_from_filename(f)
    return df


def _write_df(df: pd.DataFrame, out, header_cols: list[str], *, header: bool) -> None:
    """Escreve o df no buffer alinhado a header_cols (preenche faltantes, descarta extras)."""
    if list(df.columns) != header_cols:
        extras = [c for c in df.columns if c not in header_cols]
        if extras:
            logger.warning(f"⚠️ Colunas extras ignoradas: {extras}")
        df = df.reindex(columns=header_cols)
    df.to_csv(out, sep=";", index=False, header=header)


def _processed_ids(bronze_path: Path) -> set[str]:
    """Ids de processo ja presentes no bronze, lidos da coluna id_processo em chunks.

    O proprio bronze e a fonte da verdade do que ja foi processado (sem manifesto
    paralelo que possa divergir). Retorna conjunto vazio se o bronze nao puder ser
    usado (inexistente, vazio ou sem a coluna id_processo), sinalizando rebuild.
    """
    ids: set[str] = set()
    try:
        for chunk in pd.read_csv(
            bronze_path, sep=";", usecols=[_ID_COL], chunksize=200_000, dtype=str
        ):
            ids.update(chunk[_ID_COL].dropna().unique())
    except (
        ValueError,
        FileNotFoundError,
        pd.errors.EmptyDataError,
        pd.errors.ParserError,
    ):
        return set()
    return ids


def transform(cfg: PipelineConfig):
    """Transforma os JSONs do landing em CSV bronze de forma incremental.

    Processa apenas os processos ainda ausentes do bronze (descobertos pela coluna
    id_processo do proprio bronze) e faz append atomico via copia temporaria. Se o
    bronze nao existir/for inutilizavel, reconstroi do zero em streaming. O pico de
    memoria fica limitado a um arquivo, e o reprocessamento do historico e evitado.
    """
    bronze_path = cfg.bronze_filepath
    bronze_path.parent.mkdir(parents=True, exist_ok=True)

    processed = _processed_ids(bronze_path) if bronze_path.exists() else set()
    if not processed:
        logger.info(
            "🔄 Bronze ausente/inutilizavel: reconstruindo do zero (streaming)."
        )
        return _full_rebuild(cfg)

    header_cols = list(pd.read_csv(bronze_path, sep=";", nrows=0).columns)
    pendentes = [
        f
        for f in cfg.landing_dir.iterdir()
        if f.suffix == ".json" and _id_from_filename(f) not in processed
    ]
    if not pendentes:
        logger.info(
            f"✅ Nada novo: {len(processed)} processos ja no bronze. Nada a fazer."
        )
        return

    logger.info(
        f"🔄 Incremental: {len(pendentes)} arquivos novos "
        f"(bronze ja tem {len(processed)} processos)."
    )

    fd, tmp_name = tempfile.mkstemp(
        dir=bronze_path.parent, prefix=f".{bronze_path.stem}_", suffix=".tmp"
    )
    os.close(fd)
    try:
        # Copia o bronze atual e faz append no temporario: se a task morrer no meio,
        # o bronze fica intacto e o retry reprocessa os pendentes sem duplicar.
        shutil.copyfile(bronze_path, tmp_name)
        novos_ok = 0
        novas_linhas = 0
        with open(tmp_name, "a", encoding="utf-8", newline="") as out:
            for f in pendentes:
                try:
                    df = _parse_file(f)
                    if df is None:
                        continue
                    _write_df(df, out, header_cols, header=False)
                    novos_ok += 1
                    novas_linhas += len(df)
                    del df
                except Exception:
                    logger.error(f"❌ Erro ao transformar {f}", exc_info=True)
                    continue

        os.chmod(tmp_name, 0o664)
        os.replace(tmp_name, bronze_path)
        logger.info(
            f"✅ Incremental concluido: +{novos_ok} arquivos, "
            f"+{novas_linhas} linhas -> {bronze_path}"
        )
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)


def _full_rebuild(cfg: PipelineConfig):
    """Reconstroi o bronze do zero, em streaming (um arquivo por vez).

    Usado quando nao ha bronze reutilizavel. Escreve em temporario e substitui o
    bronze atomicamente ao final; preserva o bronze anterior se nao houver dados.
    """
    bronze_path = cfg.bronze_filepath
    header_cols: list[str] | None = None
    arquivos_ok = 0
    total_linhas = 0

    fd, tmp_name = tempfile.mkstemp(
        dir=bronze_path.parent, prefix=f".{bronze_path.stem}_", suffix=".tmp"
    )
    os.close(fd)
    try:
        with open(tmp_name, "w", encoding="utf-8", newline="") as out:
            for f in cfg.landing_dir.iterdir():
                if f.suffix != ".json":
                    continue
                try:
                    df = _parse_file(f)
                    if df is None:
                        continue
                    if header_cols is None:
                        header_cols = list(df.columns)
                        _write_df(df, out, header_cols, header=True)
                    else:
                        _write_df(df, out, header_cols, header=False)
                    arquivos_ok += 1
                    total_linhas += len(df)
                    del df
                except Exception:
                    logger.error(f"❌ Erro ao transformar {f}", exc_info=True)
                    continue

        if header_cols is None:
            logger.warning(
                "⚠️ Nenhum arquivo com dados em landing_dir. "
                "Abortando transform e preservando bronze anterior."
            )
            return

        os.chmod(tmp_name, 0o664)
        os.replace(tmp_name, bronze_path)
        logger.info(
            f"✅ Rebuild concluido: {arquivos_ok} arquivos, "
            f"{total_linhas} linhas -> {bronze_path}"
        )
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)


def run_pipeline(cfg):
    etl = GenericETL(
        cfg=cfg,
        extract_fn=extract,
        transform_fn=transform,
        load_fn=None,
        log=logger,
    )

    # etl.extract()
    etl.transform()
    etl.load()


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )
    config = load_source_config(_CONFIG_FILE, source="processo", env="local")
    run_pipeline(PipelineConfig(**config))
    # python -m src.pipelines.legislativo.senado.senado_processos
