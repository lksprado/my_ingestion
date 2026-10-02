import pytest

from pipelines.financas.investimentos.investimentos_google import (
    GoogleSheetsSettings,
)


def test_settings_da_fonte(monkeypatch, tmp_path):
    conta = tmp_path / "sa.json"
    conta.write_text("{}")
    monkeypatch.setenv("GOOGLE_CREDENTIALS_FILE", str(conta))
    monkeypatch.setenv("URL_FINANCE__LUCAS_JESSICA", "https://a")
    conf = GoogleSheetsSettings.carregar(_env_file=None)
    assert conf.google_credentials_file == conta
    assert conf.url_finance == {"lucas_jessica": "https://a"}


def test_service_account_inexistente(monkeypatch, tmp_path):
    monkeypatch.setenv("GOOGLE_CREDENTIALS_FILE", str(tmp_path / "nao_existe.json"))
    with pytest.raises(ValueError, match="GOOGLE_CREDENTIALS_FILE"):
        GoogleSheetsSettings.carregar(_env_file=None)
