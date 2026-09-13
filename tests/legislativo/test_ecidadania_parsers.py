from core.parsers.html import make_bs_object
from pipelines.legislativo.ecidadania.ecidadania_etl import (
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
