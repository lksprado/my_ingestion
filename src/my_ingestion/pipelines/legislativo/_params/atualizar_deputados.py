import pandas as pd

from my_ingestion.core.http import HttpClient


def obter_ids_deputados_atuais():
    """Funcao auxiliar para obter todos deputados atuais"""
    extractor = HttpClient()
    data = extractor.make_http_request(
        url="https://dadosabertos.camara.leg.br/api/v2/deputados?ordem=ASC&ordenarPor=nome",
        # output_dir= "src/params/",
        # filename="id_deputados.json"
    )
    df = pd.DataFrame(data["dados"])

    df.to_csv("src/params/id_deputados.csv", sep=";")
    print("Finalizado")
