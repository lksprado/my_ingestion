# Pipeline: Câmara dos Deputados

Extrai dados públicos da [API de Dados Abertos da Câmara dos Deputados](https://dadosabertos.camara.leg.br/api/v2/).
ETL em `camara_etl.py` (todas as entidades); configuração em `camara_config.yml`.

## O que coleta

| Entidade | Fonte | Tabela destino |
|---|---|---|
| `legislaturas` | Deputados da legislatura atual | `raw_camara.raw_camara_legislaturas` |
| `deputados` | Perfil de cada deputado | `raw_camara.raw_camara_deputados` |
| `votacoes` | Votações em plenário (trimestre corrente) | `raw_camara.raw_camara_votacoes` |
| `votos_deputados` | Como cada deputado votou | `raw_camara.raw_camara_votos_deputados` |
| `votos_orientacao` | Orientação de bancada por votação | `raw_camara.raw_camara_votacoes_orientacao` |
| `proposicao_tema` | Temas de cada proposição | `raw_camara.raw_camara_proposicao_tema` |
| `proposicao` | Detalhe das proposições votadas | `raw_camara.raw_camara_proposicao` |

## Dependências entre pipelines

A extração é parametrizada por CSVs de IDs (`parameter_file` no YAML), gerados por
quem vem antes na cadeia (`output_param_file`):

```
_params/atualizar_deputados.py ──> id_deputados.csv ──> deputados

votacoes ──> id_votacoes.csv ────> votos_deputados
                              └──> votos_orientacao
         └─> id_proposicao.csv ──> proposicao
                              └──> proposicao_tema
```

A ordem de `ETLS` já põe `votacoes` **antes** dos quatro que dependem dele. Rode
`atualizar_deputados` antes de `deputados` quando a legislatura mudar.

## Como executar

`camara_etl [entidade ...] [--steps extract,transform,load]`; sem entidades roda
todas na ordem acima (a falha de uma não aborta as demais).

```bash
uv run python -m pipelines.legislativo._params.atualizar_deputados   # só quando mudar a legislatura

uv run python -m pipelines.legislativo.camara.camara_etl                          # todas
uv run python -m pipelines.legislativo.camara.camara_etl votacoes votos_deputados  # só essas
uv run python -m pipelines.legislativo.camara.camara_etl proposicao --steps transform,load   # sem bater na API
```

## Notas

- **Extração por ID** (`votos_*`, `proposicao*`): `base_url` e `landing_file` usam o
  placeholder `{id}`; `core.extract_by_ids` requisita só os IDs que não estão no
  landing nem no CSV `options.no_data_file` (IDs que a API não respondeu).
- **Divergência registrada:** `votos_orientacao` tem `blacklist_on_error: false`
  (timeout **não** entra no "sem dados"); os outros quatro registram timeout como
  "sem dados" e nunca mais tentam. Decidir um comportamento único é pendência.
- **Bronze em streaming:** `votos_deputados`, `proposicao` e `proposicao_tema`
  montam o bronze um arquivo por vez (`core.write_bronze_incremental`, que cai
  no `write_bronze_streaming` quando a entidade não tem `control_table`), porque
  o landing tem milhares de JSONs e não cabe em memória de uma vez.
- Carga full refresh (`write: truncate`): `TRUNCATE` + `COPY` na tabela
  `raw_camara.*`, numa transação e sem recriar a tabela. Vale para `legislaturas`,
  `deputados` (a ficha muda com o tempo), `votacoes` (o bronze concatena todo o
  landing, então a mesma votação reaparece em mais de um arquivo) e
  `votos_orientacao`.
- `votos_deputados`, `proposicao` e `proposicao_tema` são **incrementais por
  arquivo** (`write: append` + `options.control_table`): o JSON de cada ID é
  imutável, então o transform só põe no bronze o que ainda não entrou e o load
  registra o manifesto junto com o COPY. Refazer não duplica. O ganho é no
  transform: reconstruir `votos_deputados` inteiro custa ~93 s (5.961 arquivos,
  1,9 M linhas) contra ~1 s no caminho incremental.
- Ao ligar o incremental numa tabela que já tem histórico, rode uma vez
  `uv run python scripts/controle_semear.py src/pipelines/legislativo/camara/camara_config.yml <entidade>`
  — sem isso o primeiro delta traz todo o landing e o `append` duplica a tabela.
