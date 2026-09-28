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

```

### Lacunas em `votacoes`

A rotina só olha os últimos `options.trimestres` trimestres. Se aparecer buraco
mais antigo (uma página que falhou, uma parada longa),
`scripts/camara_votacoes_backfill.py --desde <ano>` rebaixa todos os trimestres
desde aquele ano. Rode **onde está o landing que alimenta a raw** (em prod, o
container do Airflow) e depois a DAG da Câmara: `votacoes` reconstrói o bronze e
as entidades por ID buscam só os IDs novos.

Limite conhecido: votações **sem evento** (`idEvento` nulo) não saem na listagem
`/votacoes` com nenhum filtro; só em `/votacoes/{id}` e nos arquivos anuais.
Nenhuma delas é nominal.

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
  ID congela o status do primeiro download. Volume: da ordem de 1 milhão de
  proposições e mais de 1 GB no landing; o extract leva minutos. O transform vai do ano
  mais recente para o mais antigo (o esquema novo é o mais completo, então o
  bronze não precisa ser regravado para acomodar coluna nova), com o parse em
  processos paralelos (`options.transform_workers`). `ano = 0` é proposição sem
  numeração.
- **Extração por ID** (`votos_*`, `proposicao*`): `base_url` e `landing_file` usam o
  placeholder `{id}`; `core.extract_by_ids` requisita só os IDs que não estão no
  landing nem no CSV `options.no_data_file` (IDs sem dado ou com 404), em 4
  threads (`options.workers`). Se a API começar a devolver 429, baixe esse número.
  Timeout e 5xx não entram no "sem dados": o ID fica pendente, a data da primeira
  falha vai para `<no_data_file>_erros.csv`, e só depois de 7 dias falhando
  (`options.dias_para_desistir`) ele é desistido. Se nenhum ID der certo numa
  execução (API fora do ar), o extract falha.
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
  transform: reconstruir `votos_deputados` inteiro (milhões de linhas) leva
  minutos; o delta, segundos.
- Ao ligar o incremental numa tabela que já tem histórico, rode uma vez
  `uv run python scripts/controle_semear.py src/pipelines/legislativo/camara/camara_config.yml <entidade>`
  — sem isso o primeiro delta traz todo o landing e o `append` duplica a tabela.
