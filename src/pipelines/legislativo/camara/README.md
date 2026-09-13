# Pipeline: Câmara dos Deputados

Extrai dados públicos da [API de Dados Abertos da Câmara dos Deputados](https://dadosabertos.camara.leg.br/api/v2/).
Configuração em `camara_config.yml`.

## O que coleta

| Script | Source (YAML) | Fonte | Tabela destino |
|---|---|---|---|
| `camara_legislaturas.py` | `legislatura` | Lista de legislaturas | `raw_camara.legislaturas` |
| `camara_deputados.py` | `deputados` | Perfil de cada deputado | `raw_camara.deputados` |
| `camara_votacoes.py` | `votacoes` | Votações em plenário | `raw_camara.votacoes` |
| `camara_votos_deputados.py` | `votos_deputados` | Como cada deputado votou | `raw_camara.votos_deputados` |
| `camara_votos_orientacao.py` | `votos_orientacao` | Orientação de bancada por votação | `raw_camara.votos_orientacao` |
| `camara_proposicoes.py` | `proposicao` | Detalhe das proposições votadas | `raw_camara.proposicao` |
| `camara_proposicao_tema.py` | `proposicao_tema` | Temas de cada proposição | `raw_camara.proposicao_tema` |

## Dependências entre pipelines

A extração é parametrizada por CSVs de IDs (`parameter_file` no YAML), gerados por
quem vem antes na cadeia (`output_param_file`):

```
_params/atualizar_deputados.py ──> id_deputados.csv ──> camara_deputados

camara_votacoes ──> id_votacoes.csv ────> camara_votos_deputados
                                     └──> camara_votos_orientacao
                └─> id_proposicao.csv ──> camara_proposicoes
                                     └──> camara_proposicao_tema
```

Ou seja: rode `camara_votacoes` **antes** dos quatro que dependem dele, e
`atualizar_deputados` antes de `camara_deputados` quando a legislatura mudar.

## Como executar

```bash
uv run python -m pipelines.legislativo._params.atualizar_deputados   # só quando mudar a legislatura

uv run python -m pipelines.legislativo.camara.camara_legislaturas
uv run python -m pipelines.legislativo.camara.camara_deputados
uv run python -m pipelines.legislativo.camara.camara_votacoes        # primeiro: gera os IDs
uv run python -m pipelines.legislativo.camara.camara_votos_deputados
uv run python -m pipelines.legislativo.camara.camara_votos_orientacao
uv run python -m pipelines.legislativo.camara.camara_proposicoes
uv run python -m pipelines.legislativo.camara.camara_proposicao_tema
```

## Notas

- Os pipelines por ID (`votos_*`, `proposicao*`) são **incrementais**: comparam os IDs
  pendentes com o que já existe no landing e mantêm um arquivo de IDs "sem dados"
  para não repetir chamadas que a API não responde.
- Carga é full refresh (`replace`) na tabela `raw_camara.*`.
