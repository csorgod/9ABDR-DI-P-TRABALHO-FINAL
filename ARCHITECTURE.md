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
  │  Ingestão:  PySpark notebook    │     │  Ingestão:  Python + COPY INTO  │
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
  │  Transform.: PySpark + SQL      │     │  Transform.: SQL (Stored Proc)  │
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
              │  ✓  QUARENTENA — aplicada no Silver       │
              │                                          │
              │  Databricks (notebook Silver):           │
              │  • QC-B1: campos obrigatórios (Bronze)   │
              │  • QC-B2: placar não-negativo  (Bronze)  │
              │  • minuto de gol inválido → quarentena   │
              │                                          │
              │  Snowflake (SP_SILVER_TRANSFORM):        │
              │  • Mesmas regras em SQL puro             │
              │  • Falha → SILVER.*_QUARANTINE           │
              └──────────────────┬───────────────────────┘
                                 │
                                 ▼
══════════════════════════════════════════════════════════════════════════════════
  GOLD  —  Métricas prontas para consumo
══════════════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────┐     ┌─────────────────────────────────┐
  │         DATABRICKS              │     │          SNOWFLAKE              │
  │                                 │     │                                 │
  │  Modelo:   SQL + PySpark        │     │  Modelo:   SQL puro             │
  │  Update:   MERGE INTO           │     │  Update:   CREATE OR REPLACE   │
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
              │  ✓  QUALITY CHECKS — pós-Gold (Snowflake)│
              │                                          │
              │  SP_QUALITY_CHECKS → QC.CHECK_RESULTS    │
              │  • QC01–02: Bronze (campos / placar)     │
              │  • QC03–04: Silver (FK / minuto de gol)  │
              │  • QC05:    Gold  (reacted_flag íntegro) │
              └──────────────────┬───────────────────────┘
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

  FREQUÊNCIA DE EXECUÇÃO
══════════════════════════════════════════════════════════════════════════════════

  Período          Frequência     Cron                  Escopo
  ─────────────────────────────────────────────────────────────────────────
  Temporada        Semanal        0 6 * * 1 (America/SP) Bronze→Silver→Gold
  (ago – mai)      (toda seg)

  Recesso          Mensal         0 6 1 * * (America/SP) Verificação + Bronze
  (jun – jul)      (dia 1/mês)

  Correções        Sob demanda    manual                 Reprocessamento parcial
  ─────────────────────────────────────────────────────────────────────────
  Databricks:  Databricks Workflow  →  Job com cron schedule
  Snowflake:   Task unica           →  USING CRON 0 6 * * 1 America/Sao_Paulo
               (TASK_WORLDCUP_WEEKLY chama SP_RUN_PIPELINE)
```
