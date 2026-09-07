import json
import logging
from pathlib import Path
from typing import Any, Literal

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from .utils.log import logger


class Extractor:
    def __init__(self):
        self.logger = logger
        self.session = self._configure_session()

    def _configure_session(self) -> requests.Session:
        retry = Retry(
            total=3,
            status_forcelist=[403, 429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            backoff_factor=0.5,
            raise_on_status=False,
        )

        adapter = HTTPAdapter(max_retries=retry)

        session = requests.Session()
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        return session

    def make_request(
        self,
        url: str,
        mode: Literal["json", "text", "auto"] = "auto",
        timeout: int = 10,
    ) -> Any | None:
        self.logger.info("Making request...")
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        }
        try:
            response = self.session.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()

            content_type = response.headers.get("Content-Type", "").lower()

            if mode == "json":
                return response.json()

            if mode == "text":
                return response.text

            # auto
            if "application/json" in content_type:
                return response.json()

            return response.text

        except requests.exceptions.RequestException as e:
            self.logger.error(
                "Request failed",
                extra={"url": url, "error": str(e)},
                exc_info=True,
            )
            return None

    @staticmethod
    def save_json(data: Any | None, output_dir: Path, filename: str) -> Path | None:
        logger = logging.getLogger(__name__)

        output_dir = Path(output_dir)

        if data is None:
            logger.warning("No data in json to save. Returning None")
            return None

        output_dir.mkdir(parents=True, exist_ok=True)

        if not filename.endswith(".json"):
            filename = f"{filename}.json"

        filepath = output_dir / filename

        with filepath.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

        logger.info(f"Extraction complete! Data saved on: {filepath}")
        return filepath
