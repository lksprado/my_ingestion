import json
import logging
import os
import re
import shutil
import tempfile
from pathlib import Path

import pandas as pd

from core import GenericETL, PipelineConfig, load_source_config
from core.http import HttpClient
from core.text import ColumnSanitizer

logger = logging.getLogger("raw_camara_proposicao_tema")

_CONFIG_FILE = Path(__file__).parent / "camara_config.yml"
_SEM_DADOS_FILE = "sem_dados_id_proposicao_tema.csv"
_LANDING_SUFFIX = "_proposicao_tema"
# Extrai o id da proposicao a partir da url self (".../proposicoes/{id}/temas").
_PROP_ID_RE = re.compile(r"/proposicoes/([^/]+)/temas")


def extract(cfg: PipelineConfig):
    logger.info("📥 Iniciando extracao...")
    extractor = HttpClient(logger)

    todos_ids = (
        pd.read_csv(cfg.parameter_filepath)[["id_proposicao"]]
        .astype(str)
        .rename(columns={"id_proposicao": "id"})
    )

    ids_landing = pd.DataFrame(
        {
            "id": [
                f.stem.removesuffix("_proposicao_tema")
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
        f"{len(pendentes)} proposicoes pendentes "
        f"(total: {len(todos_ids)}, ja extraidas: {len(ids_landing)}, sem dados: {len(ids_sem_dados)})."
    )

    for prop_id in pendentes:
        url = f"{cfg.url_base}{prop_id}/temas"
        data = extractor.make_http_request(url)
        if data and data.get("dados"):
            extractor.save_response(
                data, cfg.landing_dir, f"{prop_id}{_LANDING_SUFFIX}.json"
            )
        else:
            if data is None:
                logger.warning(
                    f"⚠️ Erro/timeout para proposicao {prop_id}, registrando para pular nas proximas runs."
                )
            else:
                logger.warning(
                    f"⚠️ Sem dados para proposicao {prop_id}, registrando para ignorar nas proximas runs."
                )
            write_header = not sem_dados_path.exists()
            with open(sem_dados_path, "a", encoding="utf-8", newline="") as f:
                if write_header:
                    f.write("id\n")
                f.write(f"{prop_id}\n")


def _id_from_filename(f: Path) -> str:
    """Id da proposicao a partir do nome do arquivo de landing."""
    return f.stem.removesuffix(_LANDING_SUFFIX)


def _parse_file(f: Path) -> pd.DataFrame | None:
    """Le um JSON de landing e devolve o DataFrame ja sanitizado, ou None se vazio."""
    with open(f, encoding="utf-8") as fp:
        raw = json.load(fp)

    dados = raw.get("dados", [])
    if not dados:
        return None

    uri_self = next(
        (lnk["href"] for lnk in raw.get("links", []) if lnk.get("rel") == "self"),
        None,
    )
    df = pd.json_normalize(dados, sep=".")
    df["url_temas"] = uri_self
    return ColumnSanitizer(df).sanitize_columns_names().df


def _write_df(df: pd.DataFrame, out, header_cols: list[str], *, header: bool) -> None:
    """Escreve o df no buffer alinhado a header_cols (preenche faltantes, descarta extras)."""
    if list(df.columns) != header_cols:
        extras = [c for c in df.columns if c not in header_cols]
        if extras:
            logger.warning(f"⚠️ Colunas extras ignoradas: {extras}")
        df = df.reindex(columns=header_cols)
    df.to_csv(out, sep=";", index=False, header=header)


def _processed_prop_ids(bronze_path: Path) -> set[str]:
    """Ids de proposicao ja presentes no bronze, lidos da coluna url_temas em chunks.

    O proprio bronze e a fonte da verdade do que ja foi processado (sem manifesto
    paralelo que possa divergir). Retorna conjunto vazio se o bronze nao puder ser
    usado (inexistente, vazio ou sem a coluna url_temas), sinalizando rebuild.
    """
    ids: set[str] = set()
    try:
        for chunk in pd.read_csv(
            bronze_path, sep=";", usecols=["url_temas"], chunksize=200_000, dtype=str
        ):
            for url in chunk["url_temas"].dropna().unique():
                m = _PROP_ID_RE.search(url)
                if m:
                    ids.add(m.group(1))
    except (ValueError, FileNotFoundError, pd.errors.EmptyDataError):
        return set()
    return ids


def transform(cfg: PipelineConfig):
    """Transforma os JSONs do landing em CSV bronze de forma incremental.

    Processa apenas as proposicoes ainda ausentes do bronze (descobertas pela coluna
    url_temas do proprio bronze) e faz append atomico via copia temporaria. Se o
    bronze nao existir/for inutilizavel, reconstroi do zero em streaming. O pico de
    memoria fica limitado a um arquivo, e o reprocessamento do historico e evitado.
    """
    bronze_path = cfg.bronze_filepath
    bronze_path.parent.mkdir(parents=True, exist_ok=True)

    processed = _processed_prop_ids(bronze_path) if bronze_path.exists() else set()
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
            f"✅ Nada novo: {len(processed)} proposicoes ja no bronze. Nada a fazer."
        )
        return

    logger.info(
        f"🔄 Incremental: {len(pendentes)} arquivos novos "
        f"(bronze ja tem {len(processed)} proposicoes)."
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

    etl.extract()
    etl.transform()
    etl.load()


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )
    config = load_source_config(_CONFIG_FILE, source="proposicao_tema", env="local")
    run_pipeline(PipelineConfig(**config))
    # python -m src.pipelines.legislativo.camara.camara_proposicao_tema
