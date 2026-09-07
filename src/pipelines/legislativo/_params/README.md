# `_params/` — scripts auxiliares do legislativo

Scripts que **não são pipelines**: rodam sob demanda para gerar insumos que os
pipelines consomem (CSVs de parâmetros) ou que o dbt consome (seeds).

## `atualizar_deputados.py`

Busca todos os deputados atuais na API da Câmara e grava `id_deputados.csv` no
`parameter_dir` resolvido a partir do `camara_config.yml` — é o arquivo que
parametriza o pipeline `camara_deputados`.

```bash
uv run python -m pipelines.legislativo._params.atualizar_deputados
```

Rode quando a legislatura mudar ou quando houver troca de titularidade.

## `dbt_seed_maker.py`

Baixa tabelas estáticas do Senado (tipos de entes, tipos de decisão, tipos de
projetos) e grava direto na pasta `seeds/` do projeto dbt, para virarem seeds.

```bash
uv run python -c "from pipelines.legislativo._params import dbt_seed_maker; dbt_seed_maker.obter_tipo_entes()"
```

⚠️ Os destinos são **caminhos absolutos hardcoded** apontando para o repo
`demodadosdw` (`/home/lucas/workspace/demodados/demodadosdw/seeds/`). Não usam
`SEEDS_ROOT`, porque essa variável aponta para outro data warehouse (`the_dw`).
Se o repo dbt mudar de lugar, edite os caminhos no script.

Gravam com `sep=","` e `index=False` — o dbt exige esse formato para seeds.

## CSVs nesta pasta

`full_id_votacoes.csv` e `full_votos_deputados_id.csv` são listas de IDs
capturadas em execuções anteriores, mantidas como ponto de partida para
reprocessamentos.
