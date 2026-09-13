"""Pipeline de clima (ex-projeto openweather).

Descobre datas faltantes em raw.openweather_daily (high-water mark), extrai o
day_summary da OpenWeather para cada uma e consolida os JSONs em CSV no staging.
Sem carga: o orquestrador faz o upsert (mesmo desenho do energia/solar).
"""

from pathlib import Path

from core import PipelineConfig, load_source_config, setup_logger
from core.db import PostgresClient
from pipelines.clima.openweather.extraction import get_day_summary
from pipelines.clima.openweather.missing_raw import identify_and_write_missing_dates
from pipelines.clima.openweather.transforming import parsing_daily_weather
from settings import settings

logger = setup_logger(__name__)

_CONFIG_FILE = Path(__file__).parent / "openweather_config.yml"


def run_pipeline(cfg: PipelineConfig) -> None:
    control = cfg.landing_dir / cfg.options["control_file"]

    db_con = PostgresClient(db_name=settings.dw_db, log=logger).connect()
    try:
        dates = identify_and_write_missing_dates(db=db_con, output_filepath=control)
    finally:
        db_con.close()

    if not dates:
        logger.info("No missing dates to process.")
        return

    get_day_summary(
        output_path=cfg.landing_dir,
        dates_list=dates,
        token=settings.openweather_api_key,
        lat=cfg.options["lat"],
        lon=cfg.options["lon"],
        base_url=cfg.url_base,
        filename_template=cfg.landing_file,
    )
    parsing_daily_weather(cfg.landing_dir, output_name=cfg.options["output_csv"])


if __name__ == "__main__":
    run_pipeline(PipelineConfig(**load_source_config(_CONFIG_FILE, source="daily")))
    # uv run python -m pipelines.clima.openweather.run
