"""Cliente HTTP único do monorepo: ``requests.Session`` com retry/backoff.

Nenhum método levanta exceção de rede: em erro, logam e devolvem ``None``
(um ID quebrado não pode derrubar uma extração longa). Sempre teste o retorno.
Quem faz várias requisições conta as falhas e chama ``ensure_some_success`` no
fim: API fora do ar faz a etapa falhar; falha parcial só avisa.

Os logs de erro mascaram o valor de parâmetros com cara de credencial
(``appid``, ``api_key``, ``token``...): a mensagem das exceções do ``requests``
traz a URL completa, query string inclusive.
"""

import json
import logging
import os
import re
import tempfile
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

_SENSITIVE_PARAM = re.compile(
    r"([?&;\s][\w.-]*(?:key|token|secret|passw|pwd|appid|auth)[\w.-]*=)[^&\s'\"]+",
    re.IGNORECASE,
)


def redact(text: str) -> str:
    """``text`` com o valor de parâmetros sensíveis de URL trocado por ``***``."""
    return _SENSITIVE_PARAM.sub(r"\1***", str(text))


def ensure_some_success(
    total: int, failures: int, what: str = "requisição(ões)", log=None
) -> None:
    """Fim de um extract com várias requisições: nada deu certo → a etapa falha.

    Falha parcial (um ID quebrado, um ano sem arquivo) só vira WARNING: o que
    faltou fica para a próxima execução, e o resto segue para transform/load.
    Quando todas falham, a API está fora do ar, e seguir verde deixaria o load
    regravar o bronze antigo com carimbo novo.
    """
    log = log or logger
    if failures:
        log.warning(f"⚠️ {failures} de {total} {what} falharam.")
    if total and failures >= total:
        raise RuntimeError(f"Extract sem nenhum sucesso: {total} {what} falharam.")


class HttpClient:
    def __init__(
        self,
        log: logging.Logger | None = None,
        retries: int = 5,
        backoff_factor: float = 2.0,
        timeout: int = 30,
        headers: dict | None = None,
        pool_size: int = 10,
    ):
        self.logger = log or logger
        self.pool_size = pool_size
        self.timeout = timeout
        self.default_headers = headers or {"Accept": "application/json"}

        retry_strategy = Retry(
            total=retries,
            backoff_factor=backoff_factor,
            status_forcelist=[403, 429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
            raise_on_status=False,
        )
        # pool_maxsize >= threads, senão o urllib3 descarta conexões ("pool is full")
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_maxsize=pool_size)
        self.session = requests.Session()
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _send(
        self,
        url: str,
        *,
        method: Literal["GET", "POST"] = "GET",
        mode: Literal["json", "text", "auto"] = "auto",
        headers: dict | None = None,
        data: dict | None = None,
        timeout: int | None = None,
        **kwargs,
    ) -> tuple[Any | None, int | None]:
        """``(corpo, status HTTP)``; o corpo é ``None`` em erro e o status é
        ``None`` quando nem houve resposta (rede, timeout, retries esgotados)."""
        merged_headers = {**self.default_headers, **(headers or {})}
        status = None
        try:
            response = self.session.request(
                method=method,
                url=url,
                headers=merged_headers,
                data=data,
                timeout=timeout or self.timeout,
                **kwargs,
            )
            status = response.status_code
            response.raise_for_status()

            if mode == "text":
                return response.text, status
            if mode == "json":
                return response.json(), status
            content_type = response.headers.get("Content-Type", "").lower()
            if "application/json" in content_type:
                return response.json(), status
            return response.text, status
        except ValueError:
            self.logger.error(f"❌ Resposta nao e JSON: {redact(url)}")
        except requests.RequestException as e:
            self.logger.error(f"❌ Erro na requisicao: {redact(url)} --- {redact(e)}")
        return None, status

    def request(self, url: str, **kwargs) -> Any | None:
        """Requisição genérica; devolve json/text conforme ``mode`` (None em erro).

        Aceita ``method``, ``mode`` (``json``/``text``/``auto``), ``headers``,
        ``data``, ``timeout`` e os demais argumentos do ``requests``.
        """
        return self._send(url, **kwargs)[0]

    def get_json_status(self, url: str, **kwargs) -> tuple[Any | None, int | None]:
        """GET que espera JSON, com o status: separa "não existe" (404) de erro
        transitório (5xx, timeout), que o ``get_json`` devolve igual (``None``)."""
        return self._send(url, mode="json", **kwargs)

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
        """Salva um objeto JSON em ``output_dir/filename`` (sufixo .json garantido).

        Grava num temporário e troca de uma vez: um processo morto no meio não
        deixa JSON truncado, que o incremental por ID tomaria por já baixado.
        """
        if data is None:
            self.logger.warning("⚠️ Sem dados para salvar - retornando None")
            return None

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        if not filename.endswith(".json"):
            filename = f"{filename}.json"

        filepath = output_dir / filename
        fd, tmp_name = tempfile.mkstemp(
            dir=output_dir, prefix=f".{filename}.", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
            os.chmod(tmp_name, 0o664)
            os.replace(tmp_name, filepath)
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        self.logger.info(f"💾 JSON salvo em: {filepath}")
        return filepath

    def fetch_and_save(
        self, url: str, output_dir: Path | str, filename: str, **kwargs
    ) -> Path | None:
        """Requisita JSON e persiste em disco."""
        data = self.get_json(url, **kwargs)
        if data is None:
            self.logger.warning(f"⚠️ Nenhum dado de {redact(url)}")
            return None
        return self.save_json(data, output_dir, filename)

    def fetch_and_save_many(
        self,
        tasks: list[tuple[str, str]],
        output_dir: Path | str,
        workers: int = 1,
    ) -> int:
        """Requisita e persiste uma lista de (url, filename); threads se workers > 1.

        Devolve quantas tarefas falharam (para o ``ensure_some_success``).
        """

        def _one(url: str, filename: str) -> bool:
            return self.fetch_and_save(url, output_dir, filename) is not None

        if workers == 1:
            return sum(not _one(url, filename) for url, filename in tasks)

        if workers > self.pool_size:
            self.logger.warning(
                f"⚠️ workers={workers} > pool_size={self.pool_size}: conexoes "
                "excedentes serao descartadas; crie o HttpClient com pool_size maior."
            )
        self.logger.info(
            f"📥 Iniciando extracao com {workers} threads ({len(tasks)} tarefas)..."
        )
        failures = 0
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_one, url, fn): fn for url, fn in tasks}
            for fut in as_completed(futures):
                try:
                    failures += not fut.result()
                except Exception as e:
                    failures += 1
                    self.logger.error(f"❌ Erro em {futures[fut]}: {e}", exc_info=True)
        return failures
