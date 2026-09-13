# Pipeline: Câmara dos Deputados

Extrai dados públicos da [API de Dados Abertos da Câmara dos Deputados](https://dadosabertos.camara.leg.br/api/v2/).
Configuração em `camara_config.yml`; helpers do domínio em `../_common.py`.

## O que coleta

| Script | Source (YAML) | Fonte | Tabela destino |
|---|---|---|---|
| `camara_legislaturas.py` | `legislaturas` | Deputados da legislatura atual | `raw_camara.legislaturas` |
| `camara_deputados.py` | `deputados` | Perfil de cada deputado | `raw_camara.deputados` |
| `camara_votacoes.py` | `votacoes` | Votações em plenário (trimestre corrente) | `raw_camara.votacoes` |
| `camara_votos_deputados.py` | `votos_deputados` | Como cada deputado votou | `raw_camara.votos_deputados` |
| `camara_votos_orientacao.py` | `votos_orientacao` | Orientação de bancada por votação | `raw_camara.votos_orientacao` |
| `camara_proposicao.py` | `proposicao` | Detalhe das proposições votadas | `raw_camara.proposicao` |
| `camara_proposicao_tema.py` | `proposicao_tema` | Temas de cada proposição | `raw_camara.proposicao_tema` |

## Dependências entre pipelines

A extração é parametrizada por CSVs de IDs (`parameter_file` no YAML), gerados por
quem vem antes na cadeia (`output_param_file`):

```
_params/atualizar_deputados.py ──> id_deputados.csv ──> camara_deputados

camara_votacoes ──> id_votacoes.csv ────> camara_votos_deputados
                                     └──> camara_votos_orientacao
                └─> id_proposicao.csv ──> camara_proposicao
                                     └──> camara_proposicao_tema
```

Rode `camara_votacoes` **antes** dos quatro que dependem dele, e
`atualizar_deputados` antes de `camara_deputados` quando a legislatura mudar.

## Como executar

Todo script aceita `--steps extract,transform,load` (default: todas).

```bash
uv run python -m pipelines.legislativo._params.atualizar_deputados   # só quando mudar a legislatura

uv run python -m pipelines.legislativo.camara.camara_legislaturas
uv run python -m pipelines.legislativo.camara.camara_deputados
uv run python -m pipelines.legislativo.camara.camara_votacoes        # primeiro: gera os IDs
uv run python -m pipelines.legislativo.camara.camara_votos_deputados
uv run python -m pipelines.legislativo.camara.camara_votos_orientacao
uv run python -m pipelines.legislativo.camara.camara_proposicao
uv run python -m pipelines.legislativo.camara.camara_proposicao_tema

uv run python -m pipelines.legislativo.camara.camara_proposicao --steps transform,load   # sem bater na API
```

## Notas

- **Extração por ID** (`votos_*`, `proposicao*`): `base_url` e `landing_file` usam o
  placeholder `{id}`; `_common.extract_by_ids` requisita só os IDs que não estão no
  landing nem no CSV `options.no_data_file` (IDs que a API não respondeu).
- **Divergência registrada:** `votos_orientacao` tem `blacklist_on_error: false`
  (timeout **não** entra no "sem dados"); os outros quatro registram timeout como
  "sem dados" e nunca mais tentam. Decidir um comportamento único é pendência.
- **Bronze em streaming (rebuild):** `votos_deputados`, `proposicao` e
  `proposicao_tema` reconstroem o bronze inteiro a cada transform, um arquivo por
  vez (`core.write_bronze_streaming`). Antes era append incremental; o rebuild é
  mais lento por execução, mas sem estado paralelo ao landing.
- Carga é full refresh (`replace`) na tabela `raw_camara.*`.
