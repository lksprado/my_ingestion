# Pipeline: Câmara dos Deputados

Extrai dados públicos da [API de Dados Abertos da Câmara dos Deputados](https://dadosabertos.camara.leg.br/api/v2/).
ETL em `camara_etl.py` (todas as entidades); configuração em `camara_config.yml`.

## O que coleta

| Entidade | Fonte | Tabela destino |
|---|---|---|
| `legislaturas` | Deputados das legislaturas 51–57 (`options.legislaturas`) | `raw_camara.raw_camara_legislaturas` |
| `deputados` | Perfil de cada deputado (atuais + históricos) | `raw_camara.raw_camara_deputados` |
| `votacoes` | Votações (os 2 últimos trimestres; histórico via backfill) | `raw_camara.raw_camara_votacoes` |
| `votos_deputados` | Como cada deputado votou | `raw_camara.raw_camara_votos_deputados` |
| `votos_orientacao` | Orientação de bancada por votação | `raw_camara.raw_camara_votacoes_orientacao` |
| `proposicao_tema` | Temas de cada proposição | `raw_camara.raw_camara_proposicao_tema` |
| `proposicao` | Detalhe das proposições votadas | `raw_camara.raw_camara_proposicao` |
| `arquivo_proposicoes` | **Todas** as proposições, arquivo anual (1934..) | `raw_camara.arquivo_proposicoes` |
| `arquivo_proposicoes_temas` | Temas de todas as proposições, arquivo anual (1945..) | `raw_camara.arquivo_proposicoes_temas` |

## Dependências entre pipelines

A extração é parametrizada por CSVs de IDs (`parameter_file` no YAML), gerados por
quem vem antes na cadeia (`output_param_file`):

```
_params/atualizar_deputados.py ──> id_deputados.csv ─────────────┐
legislaturas ──────────────────> id_deputados_legislaturas.csv ──┴──> deputados

votacoes ──> id_votacoes.csv ────> votos_deputados
                              └──> votos_orientacao
         └─> id_proposicao.csv ──> proposicao
                              └──> proposicao_tema
```

A ordem de `ETLS` já põe `votacoes` **antes** dos quatro que dependem dele e
`legislaturas` antes de `deputados`. `deputados` sempre rebaixa a ficha dos
atuais (`id_deputados.csv`, de `atualizar_deputados`), porque ela muda; dos
históricos (`id_deputados_legislaturas.csv`, de `legislaturas`), baixa só quem
ainda não tem arquivo no landing. Sem o arquivo histórico, fica só nos atuais.
As `arquivo_*` não dependem de nada.

## Como executar

`camara_etl [entidade ...] [--steps extract,transform,load]`; sem entidades roda
todas na ordem acima (a falha de uma não aborta as demais).

```bash
uv run python -m pipelines.legislativo._params.atualizar_deputados   # só quando mudar a legislatura

uv run python -m pipelines.legislativo.camara.camara_etl                          # todas
uv run python -m pipelines.legislativo.camara.camara_etl votacoes votos_deputados  # só essas
uv run python -m pipelines.legislativo.camara.camara_etl proposicao --steps transform,load   # sem bater na API

uv run python scripts/camara_votacoes_backfill.py --desde 2001   # uma vez: preenche as lacunas de votacoes
```

### Backfill de `votacoes` (uma vez)

Até setembro de 2026 o extract pegava só o trimestre corrente e terminava a
janela no último dia dele. Isso gerava duas perdas. A `dataFim` da API é
**exclusiva**, então o último dia de todo trimestre ficava de fora. Além disso,
a DAG é semanal, e o que era registrado depois da última execução do trimestre
nunca entrava. Uma auditoria contra os arquivos anuais achou 2.978 votações
faltando, 89 delas nominais e sem votos.

O backfill tem de rodar **onde está o landing que alimenta a raw** (em prod, o
container do Airflow). Depois, dispare a DAG da Câmara: `votacoes` reconstrói o
bronze, e `votos_deputados`/`votos_orientacao`/`proposicao*` buscam só os IDs
novos. No teste de 2025, as lacunas caíram de 1.012 para 80 e as nominais sem
votos de 40 para 0.

O resto (~900 em 2001–2026) são votações **sem evento** (`idEvento` nulo). A
listagem `/votacoes` não as devolve com nenhum filtro; só aparecem em
`/votacoes/{id}` e nos arquivos anuais (`arquivos/votacoes`). Nenhuma delas é
nominal.

## Notas

- **`votacoes`**: janelas trimestrais com `dataFim` = 1º dia do trimestre
  seguinte (a API recusa intervalo maior que 3 meses e trata `dataFim` como
  exclusiva). A votação registrada exatamente à 0h da virada cai nas duas
  janelas; o transform remove a repetição por `id`.
- **`legislaturas`**: a API corta a listagem em 1000 itens (a 55 tem 1.138, a
  56 tem 1.073). O extract pagina (`itens=100`) e grava um JSON por
  legislatura com as páginas juntas; se uma página falhar, fica o arquivo
  anterior.
- **`arquivo_*`**: arquivos anuais de `dadosabertos.camara.leg.br/arquivos`, em
  tabelas próprias porque o formato difere da API por ID (`ultimoStatus.*` em
  vez de `statusProposicao.*`). Todos os anos são rebaixados a cada execução,
  porque o arquivo traz o status **atual** das proposições; o `proposicao` por
  ID congela o status do primeiro download. Volume: ~1 M de proposições,
  1,5 GB no landing, ~3,5 min. O transform vai do ano mais recente para o mais
  antigo (o cabeçalho do bronze é o do 1º arquivo), com o parse em 4 processos
  (`options.transform_workers`), ~25 s. `ano = 0` é proposição sem
  numeração.
- **Extração por ID** (`votos_*`, `proposicao*`): `base_url` e `landing_file` usam o
  placeholder `{id}`; `core.extract_by_ids` requisita só os IDs que não estão no
  landing nem no CSV `options.no_data_file` (IDs que a API não respondeu), em 4
  threads (`options.workers`). Se a API começar a devolver 429, baixe esse número.
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
