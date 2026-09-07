# webscraping-inflation

Projeto de scraping semanal de produtos do mercado próximo de onde eu moro, com o objetivo de calcular uma variação de preços mais personalizada.

## Visão geral
- Coleta semanal de preços de produtos.
- Cálculo de variação de preços personalizada.
- Modelagem e armazenamento dos dados no repositório [my_datawarehouse](https://github.com/lksprado/my_datawarehouse).
- Orquestração da extração pelo repositório [my_orchestrator](https://github.com/lksprado/my_orchestrator).

## Fluxo macro
1) Extração dos preços (este repositório).
2) Carga e modelagem no `my_datawarehouse`.
3) Orquestração e agendamento no `my_orchestrator`.

## Repositórios relacionados
- `my_datawarehouse`: modelagem e persistência dos dados.
- `my_orchestrator`: orquestração da extração e do pipeline.