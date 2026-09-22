from types import SimpleNamespace

import pandas as pd

from core.parsers.html import make_bs_object
from pipelines.legislativo.ecidadania import ecidadania_etl
from pipelines.legislativo.ecidadania.ecidadania_etl import (
    extract_paginas,
    parse_big_numbers,
    parse_materias,
)

HTML = """
<div id="container-consulta-publica">
  <div class="box-est-cp"><header>1.234</header></div>
  <div class="box-est-cp"><header>56</header></div>
  <div class="box-est-cp"><header>7.890</header></div>
  <div class="resumo-materia">
    <header><a href="/visualizacaomateria?id=1">PL 1234/2024</a></header>
    <section><a>Ementa</a></section>
    <figure class="grafico-consulta-publica"><header>
      <span>1.000</span><span>20</span>
    </header></figure>
  </div>
  <div class="resumo-materia">
    <header><a href="/x">XYZ 9/2020</a></header>
  </div>
</div>
"""


def test_parse_materias():
    df = parse_materias(make_bs_object(response=HTML))
    assert len(df) == 2
    row = df.iloc[0]
    assert (row["sigla"], row["numero"], row["ano"]) == ("PL", "1234", "2024")
    assert row["tipo_proposicao"] == "PROJETO DE LEI"
    assert (row["votos_sim"], row["votos_nao"]) == (1000, 20)
    assert row["link"].endswith("/ecidadania/visualizacaomateria?id=1")
    assert df.iloc[1]["tipo_proposicao"] == "DESCONHECIDO"
    assert (df.iloc[1]["votos_sim"], df.iloc[1]["votos_nao"]) == (0, 0)


def test_parse_big_numbers():
    df = parse_big_numbers(make_bs_object(response=HTML))
    assert df.iloc[0][
        ["total_proposicoes_votadas", "total_pessoas_votaram"]
    ].tolist() == [
        1234,
        56,
    ]


def test_parsers_empty_on_missing_container():
    soup = make_bs_object(response="<html><body>nada</body></html>")
    assert parse_materias(soup).empty
    assert parse_big_numbers(soup).empty


def _pagina(titulo: str) -> str:
    return f"""
<div id="container-consulta-publica">
  <div class="resumo-materia"><header><a href="/x">{titulo}</a></header></div>
</div>
"""


def test_extract_paginas_para_na_pagina_vazia(tmp_path, monkeypatch):
    respostas = {
        "u?p=1": _pagina("PL 1/2024"),
        "u?p=2": None,  # falha HTTP: segue para a próxima
        "u?p=3": _pagina("PL 3/2024"),
        "u?p=4": _pagina("").replace("<a", "<b"),  # sem matérias: fim
        "u?p=5": _pagina("PL 5/2024"),
    }
    pedidas = []

    class FakeHttp:
        def __init__(self, _log):
            pass

        def get_text(self, url):
            pedidas.append(url)
            return respostas[url]

    monkeypatch.setattr(ecidadania_etl, "HttpClient", FakeHttp)
    cfg = SimpleNamespace(
        url_base="u?p=",
        landing_dir=tmp_path,
        landing_file="pag_{page}.csv",
        options={"pages": 10},
    )
    extract_paginas(cfg)

    assert pedidas == ["u?p=1", "u?p=2", "u?p=3", "u?p=4"]
    assert sorted(p.name for p in tmp_path.iterdir()) == ["pag_1.csv", "pag_3.csv"]
    titulos = {pd.read_csv(p, sep=";").loc[0, "titulo"] for p in tmp_path.iterdir()}
    assert titulos == {"PL 1/2024", "PL 3/2024"}
