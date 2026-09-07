"""Parsers de HTML (ex-html_parsers.py de demodados)."""

from pathlib import Path

from bs4 import BeautifulSoup


def make_bs_object(input_file: Path = None, response: str = None) -> BeautifulSoup:
    """Cria um objeto BeautifulSoup com 'html.parser'

    Args:
        input_file (Path, optional): Abre um arquivo. Defaults to None
        response (str, optional): Recebe texto de um Response. Defaults to None.

    Returns:
        BeautifulSoup: Soup Object
    """
    if input_file is not None and response is None:
        with open(Path(input_file), encoding="utf-8") as f:
            soup = BeautifulSoup(f.read(), "html.parser")
    elif input_file is None and response is not None:
        soup = BeautifulSoup(response, "html.parser")
    else:
        raise ValueError("Ambos parametros None")
    return soup
