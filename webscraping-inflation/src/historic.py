from pathlib import Path

import pandas as pd


def make_file(input_dir: str, output_dir: str, filename: str) -> None:
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    output_file = output_path / f"{filename}.csv"

    dfs = []

    for file in input_path.iterdir():
        if file.is_file() and file.suffix == ".csv":
            df = pd.read_csv(file, sep=",", encoding="utf-8")
            dfs.append(df)

    if not dfs:
        raise ValueError("Nenhum arquivo CSV encontrado no diretório de entrada.")

    df_final = pd.concat(dfs, ignore_index=True)

    df_final.to_csv(
        output_file,
        sep=",",
        index=False,
    )


if __name__ == "__main__":
    path_minhainflacao = "/home/lucas/workspace/webscraping-inflation/data/months"
    path_atacadaohistorico = (
        "/media/lucas/Files/2.Projetos/0.mylake/bronze/atacadao_project"
    )
    output_dir = "/home/lucas/workspace/the_dw/seeds"

    make_file(
        input_dir=path_minhainflacao,
        output_dir=output_dir,
        filename="minha_inflacao",
    )

    # make_file(
    #     input_dir=path_atacadaohistorico,
    #     output_dir=output_dir,
    #     filename="atacadao_historico",
    # )
