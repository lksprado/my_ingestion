"""Cliente HTTP único do monorepo: ``requests.Session`` com retry/backoff.

Nenhum método levanta exceção de rede: em erro, logam e devolvem ``None``
(um ID quebrado não pode derrubar uma extração longa). Sempre teste o retorno.
"""

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Literal

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


class HttpClient:
    def __init__(
        self,
        log: logging.Logger | None = None,
        retries: int = 5,
        backoff_factor: float = 2.0,
        timeout: int = 30,
        headers: dict | None = None,
    ):
        self.logger = log or logger
        self.timeout = timeout
        self.default_headers = headers or {"Accept": "application/json"}

        retry_strategy = Retry(
            total=retries,
            backoff_factor=backoff_factor,
            status_forcelist=[403, 429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session = requests.Session()
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def request(
        self,
        url: str,
        *,
        method: Literal["GET", "POST"] = "GET",
        mode: Literal["json", "text", "auto"] = "auto",
        headers: dict | None = None,
        data: dict | None = None,
        timeout: int | None = None,
        **kwargs,
    ) -> Any | None:
        """Requisição genérica; devolve json/text conforme ``mode`` (None em erro)."""
        merged_headers = {**self.default_headers, **(headers or {})}
        try:
            response = self.session.request(
                method=method,
                url=url,
                headers=merged_headers,
                data=data,
                timeout=timeout or self.timeout,
                **kwargs,
            )
            response.raise_for_status()

            if mode == "text":
                return response.text
            if mode == "json":
                return response.json()
            content_type = response.headers.get("Content-Type", "").lower()
            if "application/json" in content_type:
                return response.json()
            return response.text
        except ValueError:
            self.logger.error(f"❌ Resposta nao e JSON: {url}")
        except requests.RequestException as e:
            self.logger.error(f"❌ Erro na requisicao: {url} --- {e}")
        return None

    def get_json(self, url: str, **kwargs) -> dict | list | None:
        """GET que espera JSON."""
        return self.request(url, mode="json", **kwargs)

    def get_text(self, url: str, **kwargs) -> str | None:
        """GET que devolve o corpo como texto (HTML), com User-Agent de browser."""
        headers = {"User-Agent": DEFAULT_USER_AGENT, **kwargs.pop("headers", {})}
        return self.request(url, mode="text", headers=headers, **kwargs)

    def save_json(
        self, data: Any | None, output_dir: Path | str, filename: str
    ) -> Path | None:
        """Salva um objeto JSON em ``output_dir/filename`` (sufixo .json garantido)."""
        if data is None:
            self.logger.warning("⚠️ Sem dados para salvar - retornando None")
            return None

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        if not filename.endswith(".json"):
            filename = f"{filename}.json"

        filepath = output_dir / filename
        with filepath.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        self.logger.info(f"💾 JSON salvo em: {filepath}")
        return filepath

    def fetch_and_save(
        self, url: str, output_dir: Path | str, filename: str, **kwargs
    ) -> Path | None:
        """Requisita JSON e persiste em disco."""
        data = self.get_json(url, **kwargs)
        if data is None:
            self.logger.warning(f"⚠️ Nenhum dado de {url}")
            return None
        return self.save_json(data, output_dir, filename)

    def fetch_and_save_many(
        self,
        tasks: list[tuple[str, str]],
        output_dir: Path | str,
        workers: int = 1,
    ) -> None:
        """Requisita e persiste uma lista de (url, filename); threads se workers > 1."""

        def _one(url: str, filename: str) -> None:
            self.fetch_and_save(url, output_dir, filename)

        if workers == 1:
            for url, filename in tasks:
                _one(url, filename)
            return

        self.logger.info(
            f"📥 Iniciando extracao com {workers} threads ({len(tasks)} tarefas)..."
        )
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_one, url, fn): fn for url, fn in tasks}
            for fut in as_completed(futures):
                try:
                    fut.result()
                except Exception as e:
                    self.logger.error(f"❌ Erro em {futures[fut]}: {e}", exc_info=True)
