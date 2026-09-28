import json

import pytest
import requests

from core.http import DEFAULT_USER_AGENT, HttpClient, ensure_some_success, redact


class FakeResponse:
    def __init__(self, body, content_type="application/json", status=200, url=""):
        self._body = body
        self.url = url
        self.headers = {"Content-Type": content_type}
        self.status = status

    @property
    def status_code(self):
        return self.status

    def raise_for_status(self):
        if self.status >= 400:
            raise requests.HTTPError(f"{self.status} Error for url: {self.url}")

    def json(self):
        return json.loads(self._body)

    @property
    def text(self):
        return self._body


def _client(monkeypatch, response, calls):
    def fake_request(self, **kw):
        calls.append(kw)
        return response

    monkeypatch.setattr(requests.Session, "request", fake_request)
    return HttpClient()


def test_get_json_and_get_text(monkeypatch):
    calls = []
    http = _client(monkeypatch, FakeResponse('{"a": 1}'), calls)
    assert http.get_json("http://x") == {"a": 1}
    assert calls[-1]["method"] == "GET"

    http = _client(monkeypatch, FakeResponse("<html>", "text/html"), calls)
    assert http.get_text("http://x") == "<html>"
    assert calls[-1]["headers"]["User-Agent"] == DEFAULT_USER_AGENT


def test_request_auto_mode_follows_content_type(monkeypatch):
    calls = []
    http = _client(monkeypatch, FakeResponse("oi", "text/plain"), calls)
    assert http.request("http://x") == "oi"
    http = _client(monkeypatch, FakeResponse('{"a": 1}'), calls)
    assert http.request("http://x", method="POST", data={"k": "v"}) == {"a": 1}
    assert calls[-1]["data"] == {"k": "v"}


def test_errors_return_none(monkeypatch):
    calls = []
    http = _client(monkeypatch, FakeResponse("", status=500), calls)
    assert http.get_json("http://x") is None
    http = _client(monkeypatch, FakeResponse("nao e json"), calls)
    assert http.get_json("http://x") is None


def test_save_json_and_fetch_and_save(monkeypatch, tmp_path):
    http = HttpClient()
    assert http.save_json(None, tmp_path, "x") is None
    path = http.save_json({"a": 1}, tmp_path / "sub", "x")
    assert path == tmp_path / "sub" / "x.json"
    assert json.loads(path.read_text()) == {"a": 1}

    monkeypatch.setattr(HttpClient, "get_json", lambda self, url, **kw: None)
    assert http.fetch_and_save("http://x", tmp_path, "y") is None
    monkeypatch.setattr(HttpClient, "get_json", lambda self, url, **kw: {"b": 2})
    assert http.fetch_and_save("http://x", tmp_path, "y").name == "y.json"


def test_get_json_status_separates_not_found_from_transient(monkeypatch):
    calls = []
    http = _client(monkeypatch, FakeResponse('{"a": 1}'), calls)
    assert http.get_json_status("http://x") == ({"a": 1}, 200)
    http = _client(monkeypatch, FakeResponse("", status=404), calls)
    assert http.get_json_status("http://x") == (None, 404)

    def timeout(self, **kw):
        raise requests.ConnectTimeout("timed out")

    monkeypatch.setattr(requests.Session, "request", timeout)
    assert HttpClient().get_json_status("http://x") == (None, None)


def test_error_log_masks_credentials(monkeypatch, caplog):
    url = "https://api/x?lat=1&date=2026-01-01&appid=SEGREDO"
    calls = []
    http = _client(monkeypatch, FakeResponse("", status=401, url=url), calls)
    assert http.get_json("https://api/x", params={"appid": "SEGREDO"}) is None
    assert "SEGREDO" not in caplog.text
    assert "appid=***" in caplog.text
    assert "date=2026-01-01" in caplog.text  # o resto da query continua no log


def test_redact_only_touches_sensitive_params():
    texto = "u?api_key=1&pagina=2&token=abc"
    assert redact(texto) == "u?api_key=***&pagina=2&token=***"
    assert redact("sem query") == "sem query"


def test_fetch_and_save_many_returns_failures(monkeypatch, tmp_path):
    http = HttpClient()
    monkeypatch.setattr(
        HttpClient, "get_json", lambda self, url, **kw: None if "ruim" in url else {}
    )
    tasks = [("http://ok", "a"), ("http://ruim", "b"), ("http://ok", "c")]
    assert http.fetch_and_save_many(tasks, tmp_path) == 1
    assert http.fetch_and_save_many(tasks, tmp_path, workers=2) == 1


def test_ensure_some_success():
    ensure_some_success(0, 0)
    ensure_some_success(3, 2)  # parcial: só avisa
    with pytest.raises(RuntimeError, match="nenhum sucesso"):
        ensure_some_success(3, 3)


def test_save_json_is_atomic(tmp_path):
    http = HttpClient()
    path = http.save_json({"a": 1}, tmp_path, "x")
    # Falha no meio da serialização: o arquivo anterior fica inteiro.
    with pytest.raises(TypeError):
        http.save_json({"a": object()}, tmp_path, "x")
    assert json.loads(path.read_text()) == {"a": 1}
    assert [p.name for p in tmp_path.iterdir()] == ["x.json"]


def test_pool_size_sets_adapter_pool_maxsize():
    client = HttpClient(pool_size=20)
    assert client.session.get_adapter("https://x")._pool_maxsize == 20
    assert HttpClient().session.get_adapter("https://x")._pool_maxsize == 10
