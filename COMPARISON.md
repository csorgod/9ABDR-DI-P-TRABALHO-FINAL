# Comparação: Databricks vs. Snowflake

**Responsável:** Guilherme Csorgo  
**Deadline:** 24/07/2025  
**Base:** pipelines descritos em [ARCHITECTURE.md](ARCHITECTURE.md), implementados em [databricks/](databricks/) e [snowflake/](snowflake/), sobre a mesma fonte (`openfootball/worldcup.json`) e a mesma regra de negócio (`FACT_REACTION_EVENTS`/`DIM_COMPETITION_SUMMARY`, agrupado por `competition_name`, `04_gold_insights.sql` no Snowflake e os notebooks/dbt no Databricks).

## 1. Facilidade de uso

| Critério | Databricks | Snowflake |
|---|---|---|
| Linguagem principal | PySpark (notebooks `.ipynb`) | SQL puro + Python auxiliar (`run_snowflake_sql.py`) |
| Curva de aprendizado | Exige conhecimento de Spark (DataFrames, joins, window functions, etc) | Exige só SQL (self-join, CTEs). Mais acessível para quem já sabe SQL |
| Interface de desenvolvimento | Notebook interativo (célula a célula, com prints/gráficos inline) | Scripts `.sql` numerados executados via CLI (`run_snowflake_sql.py`) ou Snowsight |
| Orquestração das camadas | Execução sequencial dos 4 notebooks | Procedures encadeadas + Tasks (`06_pipeline_procedures_and_schedule.sql`) |
| Time to market** | ~2 semanas (business hours) | ~2 semanas (business hours) |

> ** Segundo relato do grupo, os dois pipelines levaram aproximadamente o mesmo tempo (~2 semanas) para ficar prontos. O tempo de desenvolvimento não leva em consideração a expertise de cada usuário nas plataformas, então essa paridade não deve ser lida como "as duas plataformas são igualmente fáceis". É só o dado bruto de tempo gasto.

### Evidências
**PENDENTE (Membro 2 e Membro 3):** print de tela do Databricks Workspace e do Snowsight mostrando a execução de ponta a ponta, para ilustrar a diferença de experiência.

---

## 2. Ingestão / transformação

| Critério | Databricks | Snowflake |
|---|---|---|
| Ingestão Bronze | PySpark lê JSON direto, grava em Delta (`MERGE` incremental) | Python baixa/achata em CSV local, `COPY INTO` via stage interno |
| Formato de armazenamento | Delta Lake (ACID, time travel nativo) | Tabelas nativas Snowflake (ACID nativo, sem versionamento de arquivo explícito no pipeline) |
| Camada Silver | PySpark: tipagem, quarentena por regras de qualidade | SQL: tipagem (`TRY_TO_...`), quarentena pelas mesmas regras (minuto 0–130, placar não negativo) |
| Camada Gold | PySpark + SQL, self-join temporal para `reacted_flag` | SQL puro, mesma lógica de self-join (dbt models idênticos aos do Databricks) |
| Evolução de schema | Unity Catalog | Schema Evolution nativo do Snowflake |

**Observação:** a lógica de negócio da camada Gold é **literalmente igual** nos dois projetos dbt (`databricks/dbt_openfootball/` e `snowflake/dbt_openfootball/`), então a diferença aqui é puramente de engine/sintaxe, não de resultado.

**PENDENTE (Membro 2 e Membro 3):** volume de dados processado (linhas em Bronze/Silver/Gold) e tempo de execução de cada camada, para comparar throughput.

---

## 3. Performance

| Critério | Databricks | Snowflake |
|---|---|---|
| Modelo de computação | Cluster Spark (paralelismo distribuído) | Warehouse `WH_DI_P_PIPELINE`, tamanho `XSMALL`, auto-suspend em 60s |
| Adequação ao volume do projeto | "Overkill" para o volume atual somando todas as Copas do Mundo (1930–2026), o total fica na casa de **~1.000 partidas**, não dezenas de milhares. Spark compensa em volumes bem maiores que isso | Mais que suficiente para o volume atual; warehouse `XSMALL` já sobra espaço de processamento |
| Tempo de execução Medalion | Estimado: poucos minutos/semana no volume atual | Estimado: poucos minutos/semana no volume atual |
| Custo de "cold start" | Cluster leva minutos para subir (se não estiver always-on) | Warehouse XSMALL sobe em segundos |

**PENDENTE (Membro 2 e Membro 3):** tempos reais de execução (prints do histórico de jobs/queries) e tamanho do cluster/warehouse usado em cada teste — os valores acima são estimativa do grupo, não mensuração cronometrada.

### Projeção de escala (minutos → horas por semana)

O volume atual do projeto é pequeno, então o pipeline roda por **poucos minutos por semana** nas duas plataformas. Para embasar a discussão de "o que acontece se o projeto crescer", projetamos um cenário hipotético de **algumas horas de processamento por semana** (ex.: ingestão diária durante mês de Copa, enriquecimento com mais ligas/temporadas, ou granularidade maior de eventos):

| Cenário | Databricks | Snowflake |
|---|---|---|
| Atual (poucos min/semana) | Cluster sobe, processa em minutos, desliga. O custo de "esperar o cluster subir" pesa proporcionalmente mais que o processamento em si | Warehouse `XSMALL` sobe em segundos, processa, auto-suspende em 60s. quase todo o tempo cobrado é processamento de fato |
| Projetado (algumas horas/semana) | Tempo de cold start passa a ser uma fração pequena do total — a plataforma "paga menos pedágio" proporcionalmente. Cluster pode precisar de mais workers para não virar gargalo | Rodando o mesmo warehouse `XSMALL` por horas, o tempo escala de forma linear (sem pedágio de subida); se o volume exigir mais velocidade, o warehouse precisa ser redimensionado (ver custo abaixo) |

**Leitura:** em cenário de poucos minutos/semana, o *overhead* de subida do cluster Databricks pesa relativamente mais no tempo total do que no Snowflake (que sobe em segundos). Se o uso crescer para horas por semana, esse overhead relativo cai. A diferença de tempo entre as duas plataformas tende a diminuir conforme o volume/frequência cresce, porque o tempo de processamento "de verdade" passa a dominar o tempo total nas duas.

---

## 4. Governança

| Critério | Databricks | Snowflake |
|---|---|---|
| Catálogo/organização | Unity Catalog (`workspace.bronze/silver/gold`) | Banco `DI_P_MEDALLION` com schemas `BRONZE/SILVER/GOLD/QC/PIPE` |
| Linhagem (lineage) | Unity Catalog rastreia automaticamente | Rastreada manualmente via `SOURCE_FILE`, `_RUN_ID`, `QC.PIPELINE_RUNS` |
| Controle de acesso | Unity Catalog RBAC | RBAC nativo do Snowflake (roles/grants). Não implementado explicitamente nos scripts do trabalho |
| Qualidade de dados | Quarentena em tabelas Delta separadas | Quarentena em tabelas + `QC.CHECK_RESULTS`/`QC.PIPELINE_RUNS` + testes dbt |
| Dados sensíveis | N/A (base pública, sem PII) | N/A (base pública, sem PII) |

### Frequência de atualização

**Semanal (toda segunda, 06h)** na "temporada" (ago–mai); **mensal (dia 1, 06h)**, só verificação, no recesso (jun–jul); correções sob demanda. Cadência implementada em `06_pipeline_procedures_and_schedule.sql` (`PIPE.TASK_WORLDCUP_WEEKLY`) e documentada em `ARCHITECTURE.md`.

### Retenção (hot/cold por camada)

Política geral: **manter tudo**, já que o custo de storage é baixo (base cabe em dezenas de MB) e não há obrigação legal de expurgo (sem dado pessoal). Diferença é só hot vs. cold storage por idade:

| Camada | O que guardar | Retenção | Depois disso |
|---|---|---|---|
| Bronze | Arquivos brutos (JSON/TXT) | Indefinido | Cold storage após 24 meses sem acesso |
| Silver | Dado limpo consolidado | Indefinido | — |
| Silver | Versões intermediárias de migração de schema | 90 dias | Descarte |
| Gold | Versão atual das métricas | Indefinido | — |
| Gold | Versões históricas (modelos anteriores) | 12 meses | Cold storage |

Isso é relevante para a seção de custo: como a política é "manter tudo" e o volume é pequeno, o custo de storage tende a zero nas duas plataformas. A variável de custo real do projeto é compute (DBU/crédito), não armazenamento.

### Dados sensíveis (Ausencia de PII)

Mesmo sem dado sensível na base escolhida, definimos uma abordagem caso houvesse: tokenização/mascaramento antes da persistência (mapeamento em cofre externo tipo AWS Secrets Manager/Vault), anonimização irreversível na Silver, e RBAC por perfil (engenheiro de dados = acesso total; analista/cientista de dados = leitura restrita/anonimizada; consumidor de dashboard = só Gold). As duas plataformas têm suporte nativo equivalente:

| Mecanismo | Databricks | Snowflake |
|---|---|---|
| Mascaramento dinâmico | Unity Catalog Column Masks | Dynamic Data Masking Policies |
| Auditoria de acesso | Unity Catalog (log nativo) | Access History |

Isso confirma o que já estava na tabela acima (a não implementação do RBAC). Isso é uma decisão de design documentada, não há implementação disso em código rodando.

---

## 5. Custo

### Modelo de cobrança

**Databricks: DBU (Databricks Unit):**
- Cobrança por DBU consumida × preço por DBU (varia por tipo de compute e tier) + custo de infraestrutura cloud subjacente (VM do cluster). O DBU **não inclui** a VM, é cobrado em cima dela.
- Referência de mercado (2026, valores públicos de list price):  
  * **Jobs Compute (automatizado) ≈ US$ 0,15/DBU**;  
  * SQL/Data Warehousing ≈ US$ 0,22/DBU; 
  * Interactive/All-Purpose (notebook manual, como os notebooks deste projeto) ≈ US$ 0,40/DBU. até 4x mais caro que Jobs Compute pela mesma carga.
- Tier Standard está sendo descontinuado em 2026 (Os planos estão sendo migrados para premium). Premium custa ~30-37% a mais que os valores de Standard citados acima.

**Snowflake: crédito de warehouse + storage:**
- Cobrança por **crédito** consumido pelo warehouse (1 crédito/hora para `XSMALL`), dobra a cada tamanho:
  * Small: 2 
  * Medium: 4 
  * Large: 8 
  * Armazenamento (~US$ 23/TB/mês, cobrado separado do compute).
- Preço do crédito por edição (2026, list price): Standard ≈ US$ 2/crédito, Enterprise ≈ US$ 3, Business Critical ≈ US$ 4.
- Warehouse do projeto: `XSMALL`, `AUTO_SUSPEND = 60` (economiza créditos entre execuções, já que o pipeline roda 1x/semana).

> Números de mercado levantados via pesquisa pública (usamos free tier das plataformas). Ver PENDENTE abaixo para substituir por valores medidos.

### Estimativa para o cenário do grupo

O **volume atual** do projeto (poucos minutos de processamento por semana) e uma **projeção de crescimento** (algumas horas de processamento por semana, como por exemplo, se o pipeline passasse a rodar diariamente em mês de Copa com mais ligas/temporadas). Warehouse/cluster mantidos no menor tamanho (`XSMALL` / cluster mínimo) nos dois cenários, sem redimensionamento.

| Item | Databricks (Jobs Compute) | Snowflake (`XSMALL`) |
|---|---|---|
| Frequência de execução | Semanal (± diária em mês de Copa) | Semanal (± diária em mês de Copa) |
| Tempo por execução **atual** | Estimado: poucos minutos (inclui cold start do cluster) | Estimado: poucos minutos (warehouse sobe em segundos) |
| Custo por execução **atual** | ≈ US$ 0,05–0,10 (DBU + VM, poucos minutos) | ≈ US$ 0,10–0,30 (fração de 1 crédito) |
| Custo mensal **atual** (fora de Copa) | < US$ 1/mês | ≈ US$ 1–3/mês |
| Tempo por execução **projetado** (algumas horas/semana) | ~3h/semana (hipótese de trabalho) | ~3h/semana (hipótese de trabalho) |
| Custo por execução **projetado** | ≈ US$ 1,60–2,70/semana (DBU Jobs Compute + VM) | ≈ US$ 6–12/semana (3 créditos × US$2–4) |
| Custo mensal **projetado** (×4,33 semanas) | ≈ US$ 7–12/mês | ≈ US$ 26–52/mês |

**Leitura da projeção:** no volume atual (minutos/semana), o custo absoluto é irrelevante nas duas plataformas: Gastariamos poucos dólares por mês. Extrapolando para horas/semana, a diferença fica mais visível: rodando como **Jobs Compute automatizado** (não com notebook interativo), o Databricks tende a sair mais barato por hora processada do que manter um warehouse Snowflake ligado pelo mesmo tempo, porque o DBU de Jobs Compute (~US$0,15) é bem mais barato que 1 crédito Snowflake (~US$2-4) por hora equivalente de trabalho pequeno. 

> **Observação:** essa vantagem desaparece se continuarmos rodando os notebooks Databricks de forma **interativa** (como estão hoje) em vez de como Job agendado. nesse modo o DBU sobe para ~US$0,40 e a diferença encolhe bastante. Ou seja, no Databricks a escalabilidade de custo depende tanto do volume quanto da disciplina operacional (Job automatizado vs. notebook manual). no Snowflake a variável principal é só o tamanho do warehouse, que escala em degraus (dobrando a cada tier).

> **Observação 2:** Consultar `ARCHITECTURE.md` para detalhes da frequência de execução.

---

## 6. Conclusão

**PENDENTE**: Escrever após preencher as seções de performance e custo acima.**

Rascunho (a confirmar/ajustar com os números reais):
- Para o **volume atual do projeto** (~1.000 partidas no total, todas as Copas do Mundo 1930–2026, atualização semanal/mensal), Snowflake tende a ter custo/operação mais simples: warehouse pequeno, sem necessidade de gerenciar cluster Spark, SQL puro reduz a barreira de manutenção.
- Databricks se justificaria melhor se o volume crescesse por ordens de grandeza (ex.: ingestão de eventos em tempo real, múltiplas fontes de dados não estruturados) ou se o time já tivesse forte expertise em Spark/ML no mesmo workspace.
- A decisão final deve pesar não só custo/performance no volume atual, mas também para onde o grupo imagina esse pipeline crescer.
