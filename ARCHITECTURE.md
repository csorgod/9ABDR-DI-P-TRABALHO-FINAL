# Diagrama de Arquitetura — Pipeline Open Football Database

```
╔══════════════════════════════════════════════════════════════════════════════════════╗
║         PIPELINE — OPEN FOOTBALL DATABASE  │  Arquitetura Medallion                  ║
╚══════════════════════════════════════════════════════════════════════════════════════╝

  ┌──────────────────────────────────────────────┐
  │           FONTE  —  GitHub (openfootball)    │
  │                                              │
  │              worldcup.json                   │
  │      (copas do mundo de 1930 a 2026)         │
  │                                              │
  └──────────────────────────┬───────────────────┘
                             │  HTTP / Git pull
                             │  Semanal: seg 06h  |  Recesso: dia 1/mês 06h
                             ▼
══════════════════════════════════════════════════════════════════════════════════
  BRONZE  —  Dado bruto, sem transformação
══════════════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────┐     ┌─────────────────────────────────┐
  │         DATABRICKS              │     │          SNOWFLAKE              │
  │                                 │     │                                 │
  │  Ingestão:  PySpark notebook    │     │  Ingestão:  Snowpark (Python)   │
  │  Storage:   Delta Lake          │     │  Storage:   Internal Stage      │
  │  Catálogo:  Unity Catalog       │     │  Catálogo:  Snowflake DB/Schema │
  │  Orquest.:  Databricks Workflow │     │  Orquest.:  Snowflake Tasks     │
  │                                 │     │                                 │
  │  bronze.matches                 │     │  BRONZE.MATCHES                 │
  │  bronze.goals_raw               │     │  BRONZE.GOALS_RAW               │
  └───────────────┬─────────────────┘     └───────────────────┬─────────────┘
                  └──────────────┬────────────────────────────┘
                                 │
                                 ▼
══════════════════════════════════════════════════════════════════════════════════
  SILVER  —  Dado limpo, normalizado e enriquecido
══════════════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────┐     ┌─────────────────────────────────┐
  │         DATABRICKS              │     │          SNOWFLAKE              │
  │                                 │     │                                 │
  │  Transform.: PySpark + SQL      │     │  Transform.: Snowpark / SQL     │
  │  Formato:    Delta (ACID +      │     │  Formato:    Snowflake native   │
  │              time travel)       │     │              (ACID nativo)      │
  │  Schema evo: Unity Catalog      │     │  Schema evo: Schema Evolution   │
  │                                 │     │                                 │
  │  silver.matches                 │     │  SILVER.MATCHES                 │
  │  silver.matches_quarantine      │     │  SILVER.MATCHES_QUARANTINE      │
  │  silver.goal_events             │     │  SILVER.GOAL_EVENTS             │
  │  silver.goal_events_quarantine  │     │  SILVER.GOAL_EVENTS_QUARANTINE  │
  └───────────────┬─────────────────┘     └───────────────────┬─────────────┘
                  └──────────────┬────────────────────────────┘
                                 ▼
              ┌──────────────────────────────────────────┐
              │  ✓  QUALITY CHECKS — Silver              │
              │                                          │
              │  • Not-null: id, data, times, placar     │
              │  • Score não-negativo (≥ 0)              │
              │  • Validação de minutos de gols (0 a 130)│
              │                                          │
              │  Falha → registro movido para tabelas    │
              │           de quarentena específicas      │
              └──────────────────┬───────────────────────┘
                                 │
                                 ▼
══════════════════════════════════════════════════════════════════════════════════
  GOLD  —  Métricas prontas para consumo
══════════════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────┐     ┌─────────────────────────────────┐
  │         DATABRICKS              │     │          SNOWFLAKE              │
  │                                 │     │                                 │
  │  Modelo:   SQL + PySpark        │     │  Modelo:   SQL / Dynamic Tables │
  │  Update:   MERGE INTO           │     │  Update:   MERGE / Stream-based │
  │  Exposição:Databricks SQL Wh.   │     │  Exposição:Snowflake Warehouse  │
  │                                 │     │            Streamlit in SF      │
  │                                 │     │                                 │
  │  gold.fact_reaction_events      │     │  GOLD.FACT_REACTION_EVENTS      │
  │  gold.dim_competition_summary   │     │  GOLD.DIM_COMPETITION_SUMMARY   │
  └───────────────┬─────────────────┘     └───────────────────┬─────────────┘
                  └──────────────┬────────────────────────────┘
                                 │
                                 ▼
              ┌──────────────────────────────────────────┐
              │     CONSUMO  —  Pergunta de negócio      │
              │                                          │
              │  "Qual % dos times reage após sofrer um  │
              │   gol? A reação leva à vitória? Esse     │
              │   padrão varia por Copa do Mundo?"       │
              │                                          │
              │  Dashboard  /  Query analítica           │
              └──────────────────────────────────────────┘

══════════════════════════════════════════════════════════════════════════════════
  FREQUÊNCIA DE EXECUÇÃO
══════════════════════════════════════════════════════════════════════════════════

  Período          Frequência     Cron                  Escopo
  ─────────────────────────────────────────────────────────────────────────
  Temporada        Semanal        0 6 * * 1 (UTC)       Bronze→Silver→Gold
  (ago – mai)      (toda seg)

  Recesso          Mensal         0 6 1 * * (UTC)       Verificação + Bronze
  (jun – jul)      (dia 1/mês)

  Correções        Sob demanda    manual                 Reprocessamento parcial
  ─────────────────────────────────────────────────────────────────────────
  Databricks:  Databricks Workflow  →  Job com cron schedule
  Snowflake:   Task raiz com        →  USING CRON 0 6 * * 1 UTC
               Task filhas encadeadas por AFTER (Silver depois Bronze, Gold depois Silver)
```
