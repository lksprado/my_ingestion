# Home Solar: Da Coleta aos Insights
[English Version](https://github.com/lksprado/Solar/blob/main/README-en.md)

## O que é o projeto?
É um pipeline que faz login em um sistema de geração solar, extrai e transforma dados de produção de um sistema IoT solar residencial sem API pública. Este repositório foca em extração e transformação — projetado para ser acoplado como submódulo a um repositório pessoal de Airflow.

**Notas sobre o escopo**:
1. Este projeto é um submódulo de uma configuração pessoal de Airflow. A etapa de carga (load) não está intencionalmente incluída aqui.
2. Trata-se de uma configuração pessoal, não replicável, ajustada a um provedor específico.

## Por que isso existe
- Sem API oficial: Os dados ficam ocultos atrás de login e de uma visualização específica do app que habilita uma API interna.
- Engenharia prática: Demonstra scraping resiliente, transformações estruturadas e Python testável sem over-engineering.
- Analytics pessoal: Alimenta um conjunto de dados simples e consistente para visualização a jusante.

## O que ele faz
- Faz login no portal do provedor solar e busca dados históricos e atuais de produção.
- Transforma JSON bruto em DataFrames organizados e prontos para análise (resumos horários e diários).
- Escreve artefatos de controle (por exemplo, listas de datas faltantes) para garantir continuidade e idempotência entre execuções.

## Stack tecnológica
- Selenium: Automação de navegador confiável para alcançar os endpoints da API interna.
- Python + OOP: Separação clara de responsabilidades e boa manutenibilidade.
- Pytest: Testes em nível de função para componentes críticos.
- Logging: Logs estruturados para facilitar depuração e observabilidade.

## Módulos principais
- `src/missing_raw.py`: Identifica datas com dados ausentes no banco local e grava essas datas em um arquivo de controle.
- `src/extraction.py`: Autentica e obtém o JSON bruto do portal (via fluxos habilitados por Selenium).
- `src/transforming.py`: Converte JSON em DataFrames do pandas e produz agregações horárias e diárias.
- `main.py`: Exemplo de execução que conecta as etapas para uso local/debug.

Os testes associados estão em `tests/` para extração, transformação e, quando aplicável, helpers relacionados a banco de dados.

## Fluxo típico
1) Identificar lacunas: Gerar/atualizar a lista de datas faltantes.
2) Extrair dados: Fazer login, navegar até a visualização correta e requisitar o JSON por data.
3) Transformar dados: Normalizar, limpar e agregar em tabelas horárias e diárias.

O carregamento/orquestração a jusante é realizado pelo Airflow no repositório privado pai.

## Visualizações
Dashboard: https://public.tableau.com/app/profile/lucas8230/viz/HOMESOLARPANELPRODUCTION2021-2024/Painel1

![alt text](images/SUMMARY.png)
![alt text](images/DAILY.png)

