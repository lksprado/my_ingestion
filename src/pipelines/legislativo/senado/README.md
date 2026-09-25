# Pipeline: Senado Federal

Extrai dados públicos da [API de Dados Abertos do Senado Federal](https://legis.senado.leg.br/dadosabertos/).
ETL em `senado_etl.py` (todas as entidades); configuração em `senado_config.yml`.

## O que coleta

| Entidade | Fonte | Tabela destino |
|---|---|---|
| `legislaturas` | Senadores da legislatura atual | `raw_senado.raw_senado_legislaturas` |
| `senadores` | Perfil dos senadores em exercício | `raw_senado.raw_senado_senadores` |
| `votacoes` | Votações em plenário (2001..) | `raw_senado.raw_senado_votacoes` |
| `votos_senadores` | Como cada senador votou | `raw_senado.raw_senado_votos_senadores` |
| `votos_orientacao` | Orientação de bancada por votação | `raw_senado.raw_senado_votos_orientacao` |
| `processo` | Detalhe dos processos das votações | `raw_senado.raw_senado_processo` |
| `status` | Tramitação das proposições do e-Cidadania | `raw_senado.raw_senado_status` |
| `processos` | **Todos** os processos, listagem anual (1946..) | `raw_senado.processos` |

## Dependências entre pipelines

```
votacoes ──> landing de votacoes ─────────────────────────────> votos_senadores (sem extract próprio)
         └─> id_processo.csv (idprocesso) ───────────────────> processo

e-Cidadania (paginas) ──> ecidadania_paginas_consolidado.csv ──> status
```

⚠️ `status` depende de **outra fonte**: lê `ecidadania_paginas_consolidado.csv`,
o *bronze* de `ecidadania_etl paginas`, a partir do `base_parameters` do Senado. Nenhum
YAML declara esse vínculo: rode o e-Cidadania antes e copie o arquivo.

## Como executar

`senado_etl [entidade ...] [--steps extract,transform,load]`; sem entidades roda
todas na ordem acima (a falha de uma não aborta as demais).

```bash
uv run python -m pipelines.legislativo.senado.senado_etl                            # todas
uv run python -m pipelines.legislativo.senado.senado_etl votacoes votos_senadores   # só essas
uv run python -m pipelines.legislativo.senado.senado_etl status                     # depois do e-Cidadania
```

## Notas

- `votacoes` e `votos_orientacao` varrem de `options.ano_inicio` (padrão 2001)
  até o ano corrente a cada extract (`extract_by_year`). `votos_orientacao`
  começa em 2018, porque a API não tem orientação antes disso. Use `--steps`
  para rodar só uma etapa.
- `processos`: `/processo?ano=AAAA` devolve todos os processos do ano, com o
  mesmo esquema da consulta por ID de `processo`, mas em tabela própria: ~163
  mil processos, não só os ~2,6 mil votados. Todos os anos são rebaixados a
  cada execução, porque situação e tramitação mudam (o `processo` por ID
  congela o primeiro download). A resposta às vezes chega **cortada no meio**
  (`IncompleteRead`), e o retry do `HttpClient` não cobre isso; o extract
  tenta até `options.tentativas` (3) vezes por ano. Leva ~3 min.
- `status` consulta as matérias do bronze do e-Cidadania com pelo menos
  `options.min_votos` votos (YAML: 0, ou seja, as 25 páginas, ~2.500 matérias;
  antes eram só as com ≥ 5000 votos). Pula quem já está no landing fora de
  tramitação, mas quase todas as matérias do e-Cidadania ainda tramitam, então
  a maior parte é consultada de novo (~6 min com `options.workers: 4`).
- `processo` é incremental por ID no extract (`core.extract_by_ids`, default
  `has_data=bool`, `options.workers: 4` threads) e incremental por arquivo no
  transform/load (abaixo).
- Quebras de linha em colunas de texto são removidas por `core.write_bronze`.
- Carga full refresh (`write: truncate`): `TRUNCATE` + `COPY` na tabela
  `raw_senado.*`, numa transação e sem recriar a tabela.
- `processo` é **incremental por arquivo** (`write: append` +
  `options.control_table`): um JSON por processo, imutável; o transform só põe no
  bronze o que falta e o load registra o manifesto junto com o COPY.
- Ao ligar o incremental numa tabela que já tem histórico, rode uma vez
  `uv run python scripts/controle_semear.py src/pipelines/legislativo/senado/senado_config.yml processo`.
