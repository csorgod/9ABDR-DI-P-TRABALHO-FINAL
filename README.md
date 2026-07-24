# Databricks vs. Snowflake: World Cup Analytics

Projeto final de MBA em Engenharia de Dados. Construimos o mesmo pipeline de dados, com arquitetura Medallion (Bronze, Silver, Gold), em duas plataformas diferentes, Databricks e Snowflake, sobre a mesma base pública (`openfootball/worldcup.json`), para responder à mesma pergunta de negócio e depois comparar as duas plataformas entre si.

## TL;DR

**Conclusão final: SNOWFLAKE.**

Para o volume do projeto (pouco mais de mil partidas de Copa do Mundo), as duas plataformas processam os dados rápido e com custo baixo. A diferença que decide a comparação é puramente operacional: o Snowflake exige só SQL e não tem infraestrutura para gerenciar, o que reduz a barreira de entrada e dá um modelo de custo mais previsível (por tamanho). O Databricks, rodando em Serverless, também ficou simples de operar e tende a ser mais barato num cenário de crescimento de volume, mas isso depende de disciplina operacional (rodar como job automatizado, não como jupyter notebook). O detalhamento completo da comparação, com números reais das duas execuções, está em [COMPARISON.md](COMPARISON.md).

## Pergunta de negócio

Qual percentual dos times reage após sofrer um gol, e como essa taxa evoluiu ao longo das edições da Copa do Mundo? A base, a justificativa e o dicionário de dados completo estão em [FOUNDATION.md](FOUNDATION.md).

## Estrutura do repositório

```
.
├── HOMEWORK.md           Divisão do trabalho entre os membros do grupo
├── FOUNDATION.md         Base de dados escolhida e justificativa
├── ARCHITECTURE.md       Diagrama de arquitetura das duas plataformas lado a lado
├── GOVERNANCE.md         Governança, sustentabilidade e tratamento de dados sensíveis
├── COMPARISON.md         Comparação final entre Databricks e Snowflake, com conclusão
├── databricks/           Pipeline completo em Databricks (notebooks, dbt, evidências)
└── snowflake/            Pipeline completo em Snowflake (scripts SQL, dbt, evidências)
```

## Onde está cada etapa do trabalho

A divisão de tarefas está em [HOMEWORK.md](HOMEWORK.md). Cada etapa corresponde a um destes arquivos ou pastas:

| Etapa                                                   | Responsável                | Onde está                                                                                                                                            |
| ------------------------------------------------------- | --------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| Base de dados e fundação do projeto                   | Csorgo                      | [FOUNDATION.md](FOUNDATION.md)                                                                                                                         |
| Pipeline no Databricks                                  | Ludmilla e Thiago Guilherme | [databricks/](databricks/), notebooks em [databricks/notebooks/](databricks/notebooks/), evidências em [databricks/evidencias/](databricks/evidencias/) |
| Pipeline no Snowflake                                   | Ayrton                      | [snowflake/](snowflake/), scripts numerados de `01` a `07`, evidências em [snowflake/evidencias/](snowflake/evidencias/)                           |
| Comparação e conclusão                               | Csorgo                      | [COMPARISON.md](COMPARISON.md)                                                                                                                         |
| Governança, sustentabilidade e diagrama de arquitetura | Karen                       | [GOVERNANCE.md](GOVERNANCE.md) e [ARCHITECTURE.md](ARCHITECTURE.md)                                                                                     |

Cada pipeline também tem seu próprio README com instruções de execução: [databricks/README.md](databricks/README.md) e [snowflake/README.md](snowflake/README.md).

## Regra de negócio compartilhada

As duas plataformas implementam a mesma lógica na camada Gold (`FACT_REACTION_EVENTS` e `DIM_COMPETITION_SUMMARY`, agrupado por edição da Copa), inclusive com modelos dbt equivalentes em [databricks/dbt_openfootball/](databricks/dbt_openfootball/) e [snowflake/dbt_openfootball/](snowflake/dbt_openfootball/). Isso foi usado como prova de que a regra de negócio é agnóstica de plataforma, e confirmado com dado real: as duas chegaram a exatamente 75 reações em cada uma das edições de 2014, 2018 e 2022. Os detalhes dessa validação, incluindo uma divergência de volume entre as duas execuções, estão documentados em [COMPARISON.md](COMPARISON.md).
