import pytest

from pipelines.livros.google_books.google_books_etl import (
    BuscaError,
    autor_confere,
    buscar_livro,
    chave,
    escolher_item,
    parse_landing,
    parse_volume,
    slug,
)


def item(id_, title, authors=None, language=None):
    info = {"title": title}
    if language is not None:
        info["language"] = language
    if authors is not None:
        info["authors"] = authors
    return {"id": id_, "volumeInfo": info}


ITEM = {
    "id": "abc123",
    "volumeInfo": {
        "title": "Dom Casmurro",
        "authors": ["Machado de Assis"],
        "publisher": "Editora X",
        "publishedDate": "1899",
        "pageCount": 256,
        "categories": ["Fiction"],
        "language": "pt",
        "industryIdentifiers": [
            {"type": "ISBN_10", "identifier": "8535910697"},
            {"type": "ISBN_13", "identifier": "9788535910698"},
        ],
        "imageLinks": {"thumbnail": "http://books.google.com/thumb"},
        "infoLink": "http://books.google.com/info",
    },
}


class FakeHttp:
    """Responde por ``q``; consulta sem entrada devolve ``{"totalItems": 0}``."""

    def __init__(self, respostas):
        self.respostas = respostas
        self.queries = []

    def get_json(self, url, params=None):
        self.queries.append(params["q"])
        return self.respostas.get(params["q"], {"totalItems": 0})


def test_slug_e_chave():
    assert slug("O Hobbit: Lá e de Volta!") == "o_hobbit_la_e_de_volta"
    assert chave("Antifrágil", "Nassim N. Taleb") == "antifragil__nassim_n_taleb"
    assert chave("Antifrágil", None) == "antifragil__"


@pytest.mark.parametrize(
    ("autor", "resposta", "esperado"),
    [
        ("Leo Tolstoi", ["Leo Tolstoy"], True),
        ("Leo Tolstoi", ["Lev Tolstoj"], True),
        ("Machado de Assis", ["Joaquim Maria Machado de Assis"], True),
        ("Karen Maccreadi, Tim Phillips", ["Tim Phillips"], True),
        ("aristóteles", ["Aristóteles"], True),
        ("Robert D. Kaplan", None, False),
        ("David Benioff", ["Tim Ferriss"], False),
    ],
)
def test_autor_confere(autor, resposta, esperado):
    assert autor_confere(autor, resposta) is esperado


def test_buscar_livro_aceita_na_primeira_tentativa():
    http = FakeHttp(
        {
            'intitle:"Dom Casmurro" inauthor:"Machado de Assis"': {
                "items": [item("a", "Dom Casmurro", ["Machado de Assis"], "pt-BR")]
            }
        }
    )
    r = buscar_livro(http, "u", "Dom Casmurro", "Machado de Assis")
    assert (r.busca, r.indice) == ("autor", 0)
    assert len(http.queries) == 1


def test_buscar_livro_pula_item_de_outro_autor():
    http = FakeHttp(
        {
            'intitle:"Cidade de ladrões" inauthor:"David Benioff"': {
                "items": [
                    item("x", "Cidade", ["Tim Ferriss"]),
                    item("y", "Cidade de Ladrões", ["David Benioff"]),
                ]
            }
        }
    )
    r = buscar_livro(http, "u", "Cidade de ladrões", "David Benioff")
    assert r.indice == 1


def test_buscar_livro_cai_para_so_titulo():
    http = FakeHttp(
        {
            'intitle:"anna karenina"': {
                "items": [item("r", "Anna Karenina", ["graf Leo Tolstoy"])]
            }
        }
    )
    r = buscar_livro(http, "u", "anna karenina", "Leo Tolstoi")
    assert r.busca == "titulo"
    assert http.queries == [
        'intitle:"anna karenina" inauthor:"Leo Tolstoi"',
        'intitle:"anna karenina" inauthor:tolstoi',
        'intitle:"anna karenina"',
    ]


def test_buscar_livro_nenhum_confere():
    http = FakeHttp(
        {'intitle:"a mente trágica"': {"items": [item("z", "La mentalidad", None)]}}
    )
    assert buscar_livro(http, "u", "a mente trágica", "Robert D. Kaplan") is None


@pytest.mark.parametrize(
    "title",
    [
        "As seis lições (resumo)",
        "Lolita de Vladimir Nabokov (Análise do livro)",
        "Box - Fiódor Dostoiévski - Memórias da casa dos mortos e O idiota",
        "Sammlung",
    ],
)
def test_escolher_item_descarta_termos_excluidos(title):
    items = [item("x", title, ["Fulano Silva"], "pt-BR")]
    assert escolher_item(items, "Fulano Silva") is None


def test_escolher_item_prefere_portugues():
    items = [
        item("es", "Técnicas", ["Curzio Malaparte"], "es"),
        item("pt", "Técnica", ["Curzio Malaparte"], "pt-BR"),
    ]
    assert escolher_item(items, "curzio malaparte") == 1


def test_buscar_livro_procura_portugues_nas_tentativas_seguintes():
    http = FakeHttp(
        {
            'intitle:"técnicas" inauthor:"curzio malaparte"': {
                "items": [item("es", "Técnicas", ["Curzio Malaparte"], "es")]
            },
            'intitle:"técnicas"': {
                "items": [item("pt", "Técnicas", ["Curzio Malaparte"], "pt-BR")]
            },
        }
    )
    r = buscar_livro(http, "u", "técnicas", "curzio malaparte")
    assert (r.busca, r.resposta["items"][r.indice]["id"]) == ("titulo", "pt")


def test_buscar_livro_sem_portugues_usa_primeiro_aceito():
    http = FakeHttp(
        {
            'intitle:"lion rampant" inauthor:"robert woollcombe"': {
                "items": [item("en", "Lion Rampant", ["Robert Woollcombe"], "en")]
            }
        }
    )
    r = buscar_livro(http, "u", "lion rampant", "robert woollcombe")
    assert (r.busca, r.indice) == ("autor", 0)
    assert len(http.queries) == 3


def test_buscar_livro_sem_autor_aceita_primeiro():
    http = FakeHttp({'intitle:"retórica"': {"items": [item("a", "Retórica", [])]}})
    r = buscar_livro(http, "u", "retórica", None)
    assert (r.busca, r.indice) == ("titulo", 0)


def test_buscar_livro_erro_http():
    class Falha:
        def get_json(self, url, params=None):
            return None

    with pytest.raises(BuscaError):
        buscar_livro(Falha(), "u", "x", "y")


def test_parse_volume_completo():
    livro = parse_volume(ITEM)
    assert livro["authors"] == "Machado de Assis"
    assert livro["isbn_13"] == "9788535910698"
    assert livro["thumbnail"] == "http://books.google.com/thumb"
    assert livro["subtitle"] is None


def test_parse_volume_sem_identificadores_nem_autores():
    livro = parse_volume({"id": "x", "volumeInfo": {"title": "Sem dados"}})
    assert livro["authors"] is None
    assert livro["isbn_13"] is None


def test_parse_landing_usa_item_aceito():
    df = parse_landing(
        {
            "titulo": "dom casmurro",
            "autor": "Machado de Assis",
            "busca": "sobrenome",
            "indice": 1,
            "resposta": {"items": [item("x", "Outro", ["Fulano"]), ITEM]},
        }
    )
    row = df.iloc[0]
    assert (row["titulo"], row["autor"], row["busca"]) == (
        "dom casmurro",
        "Machado de Assis",
        "sobrenome",
    )
    assert row["id"] == "abc123"


def test_settings_da_fonte(monkeypatch):
    from pipelines.livros.google_books.google_books_etl import GoogleBooksSettings

    monkeypatch.setenv("GOOGLE_BOOKS_API_KEY", "k")
    assert GoogleBooksSettings.carregar(_env_file=None).api_key == "k"
    monkeypatch.delenv("GOOGLE_BOOKS_API_KEY")
    with pytest.raises(ValueError, match="GOOGLE_BOOKS_API_KEY"):
        GoogleBooksSettings.carregar(_env_file=None)
