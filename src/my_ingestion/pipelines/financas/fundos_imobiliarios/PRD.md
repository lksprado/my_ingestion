# PRD — Coleta Mensal de Dados de Fundos Imobiliários (Investidor 10)

**Versão:** 1.0  
**Data:** 2026-05-30  
**Status:** Draft

---

## 1. Visão Geral

Construir um pipeline de coleta mensal de dados de Fundos Imobiliários (FIIs) a partir do site Investidor 10, gerando arquivos organizados por mês e um histórico consolidado que cresce incrementalmente a cada execução.

A primeira extração completa foi realizada em abril/2026 (referência: dados de abril no diretório `data/indicadores/abril/`). O projeto já conta com código funcional de extração e parsing, mas carece de automação, organização temporal e consolidação dos dados.

---

## 2. Problema

O código atual funciona, mas foi escrito para execuções pontuais e manuais:

- Caminhos de saída são hardcoded (ex: `data/investidor_10_fii_list.csv`) e foram renomeados manualmente com sufixos de mês (`_abril`, `_maio`).
- Não existe mecanismo de execução periódica nem de acumulação automática de histórico.
- A função `make_df()` tem referência de variável com bug (usa `f` fora do escopo do loop) e lógica de coluna hardcoded para o ano corrente.
- Selenium roda sem modo headless, exigindo ambiente com interface gráfica.
- O limite de páginas da listagem (4 páginas) é hardcoded sem validação de completude.

---

## 3. Objetivo

Transformar o projeto em um pipeline mensal robusto que:

1. Execute automaticamente no início de cada mês, referenciando o mês de execução.
2. Salve os arquivos de saída em estrutura de diretórios organizada por `YYYY/MM`.
3. Consolide mensalmente os dados de todos os fundos em um único arquivo histórico cumulativo.
4. Seja executável tanto localmente quanto via agendamento (cron ou similar).

---

## 4. Fontes de Dados

| Fonte | Método | O que coleta |
|---|---|---|
| `investidor10.com.br/fiis/?page={n}` | HTTP + BeautifulSoup | Lista de FIIs com indicadores resumidos (P/VP, DY, liquidez, variações) |
| `investidor10.com.br/fiis/{ticker}/` | Selenium + BeautifulSoup | Indicadores históricos anuais por fundo (tabela `#table-indicators-history`) |
| Yahoo Finance (`yfinance`) | API | Série histórica de preços de fechamento e proventos pagos |

---

## 5. Estrutura de Dados de Saída

### 5.1 Estrutura de diretórios

```
data/
├── YYYY/
│   └── MM/
│       ├── fii_list.csv              # Lista de FIIs com indicadores do mês
│       ├── fii_history.csv           # Preços e proventos (yfinance)
│       └── indicadores/
│           ├── {TICKER}.csv          # Indicadores históricos por fundo
│           └── ...
├── consolidated/
│   ├── fii_list_history.csv          # Histórico acumulado da lista mensal
│   └── indicadores_history.csv       # Histórico acumulado de indicadores
```

### 5.2 Schema: `fii_list.csv`

| Coluna | Descrição |
|---|---|
| `extraction_month` | Mês de referência da extração (YYYY-MM) |
| `ticker` | Código do fundo (ex: KNCR11) |
| `net_worth` | Patrimônio líquido |
| `p_vp` | Preço sobre valor patrimonial |
| `dividend_yield_last_12_months` | DY últimos 12 meses |
| `dividend_yield_last_5_years` | DY médio últimos 5 anos |
| `daily_liquidity` | Liquidez diária |
| `fii_type` | Tipo de fundo (Papel, Tijolo, etc.) |
| `name_segment` | Segmento |
| `variation_12_months` | Variação de preço em 12 meses |
| `two_years_variation` | Variação de preço em 2 anos |
| `variation_5_years` | Variação de preço em 5 anos |

### 5.3 Schema: `indicadores/{TICKER}.csv`

| Coluna | Descrição |
|---|---|
| `extraction_month` | Mês de referência da extração (YYYY-MM) |
| `ticker` | Código do fundo |
| `indicator` | Nome do indicador (ex: P/VP, Dividend Yield) |
| `Atual` | Valor corrente no momento da extração |
| `{YYYY}` | Valor anual para cada ano disponível |

### 5.4 Schema: `fii_history.csv`

| Coluna | Descrição |
|---|---|
| `ticker` | Código do fundo |
| `date` | Data da cotação |
| `close` | Preço de fechamento |
| `dividend` | Provento pago na data (se houver) |

### 5.5 Schema: `consolidated/fii_list_history.csv`

Concatenação de todos os `fii_list.csv` mensais, com coluna `extraction_month`. Permite rastrear a evolução mensal dos indicadores resumidos de todos os fundos.

---

## 6. Comportamento Esperado do Pipeline

### 6.1 Fluxo de execução

```
1. Determinar mês de referência (ano/mês da execução atual)
2. Criar diretório de saída  data/{YYYY}/{MM}/
3. Coletar lista de FIIs  →  data/{YYYY}/{MM}/fii_list.csv
4. Coletar indicadores históricos por fundo  →  data/{YYYY}/{MM}/indicadores/{TICKER}.csv
5. Coletar histórico de preços e proventos  →  data/{YYYY}/{MM}/fii_history.csv
6. Consolidar: append ao arquivo  data/consolidated/fii_list_history.csv
7. Consolidar: append ao arquivo  data/consolidated/indicadores_history.csv
```

### 6.2 Idempotência

Se o diretório do mês já existir e os arquivos principais já estiverem presentes, o pipeline deve:
- Pular a extração daquele mês (modo skip) ou
- Sobrescrever com aviso explícito (modo force, via flag `--force`)

Isso evita duplicatas no consolidado em caso de re-execução.

### 6.3 Consolidação incremental

O arquivo consolidado acumula dados mês a mês. A coluna `extraction_month` (formato `YYYY-MM`) é a chave temporal. Ao adicionar um novo mês, o pipeline verifica se aquele `extraction_month` já está presente no consolidado antes de fazer o append.

---

## 7. Interface de Execução

### 7.1 CLI mínima

```bash
# Executa o mês atual
python main.py

# Executa um mês específico (útil para backfill)
python main.py --month 2026-03

# Força re-extração mesmo que arquivos já existam
python main.py --month 2026-04 --force

# Apenas consolida sem re-extrair
python main.py --consolidate-only
```

### 7.2 Agendamento mensal (cron)

```cron
# Executa no dia 5 de cada mês às 06:00
0 6 5 * * cd /path/to/fundos-imobiliarios && uv run python main.py >> logs/pipeline.log 2>&1
```

O dia 5 garante que os dados do mês anterior estejam disponíveis e estáveis no Investidor 10.

---

## 8. Requisitos Técnicos

### 8.1 Scraping da listagem

- Paginar automaticamente até não haver mais resultados (não depender de limite hardcoded de 4 páginas).
- Manter retry com backoff já implementado no `Extractor`.
- Adicionar delay entre requisições para não sobrecarregar o servidor.

### 8.2 Scraping de indicadores por fundo (Selenium)

- Usar modo headless (`--headless=new`) para compatibilidade com ambientes sem interface gráfica (servidores, cron).
- Fechar o driver ao final, mesmo em caso de erro.
- Salvar HTML bruto opcional para debug (flag `--save-html`).
- Implementar timeout e retry por ticker, registrando em log os que falharem sem interromper o pipeline.

### 8.3 Coleta via yfinance

- Definir `start` como a data de início do histórico disponível (ex: `2020-01-01`) e deixar o pipeline buscar apenas o incremento do período ainda não coletado, usando o arquivo consolidado como referência.
- Tratar tickers sem dados no Yahoo Finance (registrar em log, não interromper).

### 8.4 Logging

- Um arquivo de log por execução: `logs/{YYYY-MM}.log`
- Níveis: INFO para progresso normal, WARNING para dados ausentes, ERROR para falhas de extração.
- Resumo ao final: total de fundos processados, falhas, tempo de execução.

### 8.5 Dependências

Manter o stack atual: `requests`, `beautifulsoup4`, `selenium`, `pandas`, `yfinance`.  
Adicionar `lxml` como parser alternativo ao BeautifulSoup para maior performance.

---

## 9. Fora de Escopo

- Interface web ou dashboard de visualização.
- Armazenamento em banco de dados (o formato CSV é suficiente para o histórico local).
- Coleta de dados intraday ou em frequência maior que mensal.
- Alertas ou notificações de execução.
- Autenticação no site Investidor 10 (dados públicos apenas).

---

## 10. Critérios de Aceitação

| # | Critério |
|---|---|
| 1 | A execução sem argumentos gera arquivos no diretório `data/YYYY/MM/` correspondente ao mês corrente |
| 2 | O arquivo `consolidated/fii_list_history.csv` cresce com novas linhas a cada execução sem duplicar meses já presentes |
| 3 | Re-execução com `--force` sobrescreve os arquivos mensais e atualiza o consolidado corretamente |
| 4 | `--month 2026-03` executa backfill de um mês passado sem afetar outros meses no consolidado |
| 5 | O Selenium roda em modo headless sem abrir janela de browser |
| 6 | Falha em um ticker individual não interrompe o pipeline; o erro é registrado em log |
| 7 | O log ao final informa: fundos processados com sucesso, fundos com falha, e tempo total |
| 8 | O pipeline completo (lista + indicadores de ~270 fundos + yfinance) conclui em menos de 2 horas |

---

## 11. Backlog de Implementação

Ordem sugerida de desenvolvimento:

1. **Refatorar `main.py`** com roteamento por argumento CLI e lógica de mês dinâmico.
2. **Corrigir `make_df()`** — bug de escopo da variável `f` e lógica de coluna hardcoded.
3. **Adicionar modo headless** no Selenium e gestão de ciclo de vida do driver.
4. **Implementar paginação automática** da lista de FIIs (sem limite hardcoded).
5. **Implementar consolidação incremental** com checagem de `extraction_month` existente.
6. **Adicionar logging por execução** com resumo ao final.
7. **Escrever testes** para os parsers (`parse_inv10_rankings_table`, `parse_inv10_fund_table`) usando fixtures HTML locais.
8. **Documentar** o processo de agendamento via cron no `README.md`.