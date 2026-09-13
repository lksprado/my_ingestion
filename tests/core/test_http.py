import json

import requests

from core.http import DEFAULT_USER_AGENT, HttpClient


class FakeResponse:
    def __init__(self, body, content_type="application/json", status=200):
        self._body = body
        self.headers = {"Content-Type": content_type}
        self.status = status

    def raise_for_status(self):
        if self.status >= 400:
            raise requests.HTTPError(f"{self.status}")

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
