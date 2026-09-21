# `_params/` — scripts auxiliares do legislativo

Scripts que **não são pipelines**: rodam sob demanda para gerar insumos que os
pipelines consomem (CSVs de parâmetros) ou que o dbt consome (seeds).

## `atualizar_deputados.py`

Busca todos os deputados atuais na API da Câmara e grava `id_deputados.csv`
(coluna `id`, separador `,`) no `parameter_dir` resolvido a partir do
`camara_config.yml` — é o arquivo que parametriza o entidade `deputados` da Câmara.

```bash
uv run python -m pipelines.legislativo._params.atualizar_deputados
```

Rode quando a legislatura mudar ou quando houver troca de titularidade.

## `dbt_seed_maker.py`

Baixa tabelas estáticas do Senado (tipos de entes, tipos de decisão, tipos de
projetos) e grava na pasta `seeds/` do projeto dbt (`settings.seeds_root`, do
`SEEDS_ROOT`), com o nome que o dbt referencia: `seed_senado_tipos_*`.

```bash
uv run python -m pipelines.legislativo._params.dbt_seed_maker                            # todas
uv run python -m pipelines.legislativo._params.dbt_seed_maker seed_senado_tipos_entes    # uma
uv run python -m pipelines.legislativo._params.dbt_seed_maker seed_senado_tipos_decisao --forcar
```

⚠️ A API do Senado às vezes **encolhe**: uma sigla sai da lista sem nada a
substituir. Como o seed é a fonte de verdade do join no dbt, perder linha é
perder descrição em dado histórico — por isso, quando o conteúdo baixado tem
menos linhas que o seed atual, o script avisa e **não grava**. Confira o que
saiu e use `--forcar` quando a redução for esperada. (Hoje é o caso de
`tipos_decisao`: a API devolve 32 linhas e o seed tem 36.)

`seed_camara_tipos_proposicao.csv` não sai daqui — é mantido à mão no
`my_analytics`.

As mesmas tabelas existem como resíduo na raw (`raw_senado.raw_senado_tipos_*`,
`raw_camara.raw_camara_tipos_proposicao`), de um caminho antigo em que iam para
o banco em vez de virarem seed. Não têm consumidor — o dbt lê os seeds — e não
têm `loaded_at_utc`, então o `raw_copy.sh` as copia sempre por inteiro. Para
limpar: `scripts/drop_tabelas_orfas.sh {models|prod}`, que **simula por padrão**
e só derruba com `--confirmar`, pulando qualquer tabela que tenha dependente ou
cujo dado não esteja todo preservado no seed.

Grava com `sep=","` e `index=False` — o dbt exige esse formato para seeds.

## CSVs nesta pasta

`full_id_votacoes.csv` e `full_votos_deputados_id.csv` são listas de IDs
capturadas em execuções anteriores, mantidas como ponto de partida para
reprocessamentos.
