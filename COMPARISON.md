# Comparação: Databricks vs. Snowflake

**Base:** pipelines descritos em [ARCHITECTURE.md](ARCHITECTURE.md), implementados em [databricks/](databricks/) e [snowflake/](snowflake/), sobre a mesma fonte (`openfootball/worldcup.json`) e a mesma regra de negócio (`FACT_REACTION_EVENTS`/`DIM_COMPETITION_SUMMARY`, agrupado por `competition_name`, implementada em `04_gold_insights.sql` no Snowflake e nos notebooks/dbt no Databricks).

## 1. Facilidade de uso

| Critério | Databricks | Snowflake |
|---|---|---|
| Linguagem principal | PySpark (notebooks `.ipynb`) | SQL puro + Python auxiliar (`run_snowflake_sql.py`) |
| Curva de aprendizado | Exige conhecimento de Spark (DataFrames, joins, window functions, etc) | Exige só SQL (self-join, CTEs). Mais acessível para quem já sabe SQL |
| Interface de desenvolvimento | Notebook interativo (célula a célula, com prints/gráficos inline) | Scripts `.sql` numerados executados via CLI (`run_snowflake_sql.py`) ou Snowsight |
| Orquestração das camadas | Execução sequencial dos 4 notebooks | Procedures encadeadas + Tasks (`06_pipeline_procedures_and_schedule.sql`) |
| Time to market** | ~2 semanas (business hours) | ~2 semanas (business hours) |

> ** Os dois pipelines levaram aproximadamente o mesmo tempo de desenvolvimento, cerca de duas semanas, para ficar prontos. Esse tempo não leva em conta a expertise prévia de cada integrante em cada plataforma, portanto essa paridade não deve ser lida como evidência de que as duas plataformas são igualmente fáceis de aprender. É apenas o tempo bruto de desenvolvimento.

### Evidências

As evidencias completas estão disponiveis na pasta dos respectivos pipelines, dentro de `evidencias`.

#### Databricks
![Implementação databricks](databricks/evidencias/img/44b87736-0152-4ab5-8b69-9b7cb9403aff.png)

#### Snowflake
![Implementação snowflake](snowflake/evidencias/img/)

---

## 2. Ingestão / transformação

| Critério | Databricks | Snowflake |
|---|---|---|
| Ingestão Bronze | PySpark lê JSON direto, grava em Delta (`MERGE` incremental) | Python baixa/achata em CSV local, `COPY INTO` via stage interno |
| Formato de armazenamento | Delta Lake (ACID, time travel nativo) | Tabelas nativas Snowflake (ACID nativo, sem versionamento de arquivo explícito no pipeline) |
| Camada Silver | PySpark: tipagem, quarentena por regras de qualidade | SQL: tipagem (`TRY_TO_...`), quarentena pelas mesmas regras (minuto 0–130, placar não negativo) |
| Camada Gold | PySpark + SQL, self-join temporal para `reacted_flag` | SQL puro, mesma lógica de self-join (dbt models idênticos aos do Databricks) |
| Evolução de schema | Unity Catalog | Schema Evolution nativo do Snowflake |

OBS: A lógica de negócio da camada Gold é idêntica nos dois projetos dbt (`databricks/dbt_openfootball/` e `snowflake/dbt_openfootball/`). A diferença entre as duas implementações está apenas na sintaxe do motor de banco, não no resultado final.

**Volume real processado (Databricks, evidência em `databricks/evidencias/`):**

| Tabela | Linhas |
|---|---|
| `bronze.matches` (partidas) | 1.067 |
| `bronze.goals_raw` (gols brutos) | 1.195 |
| `silver.goal_events` (gols limpos) | 1.195 |
| `silver.goal_events_quarantine` (rejeitados) | 0 |
| `gold.fact_reaction_events` | 1.195 |

O volume total processado soma 1.067 partidas e 1.195 eventos de gol, sem nenhum registro em quarentena. Para a fonte `worldcup.json`, as regras de qualidade de dados não rejeitaram nenhum evento de gol.

**Pendente:** contagem de linhas por camada no Snowflake.

### Evidencias

As evidencias completas estão disponiveis na pasta dos respectivos pipelines, dentro de `evidencias`.

#### Databricks
![Implementação databricks](databricks/evidencias/img/7b5df094-dca6-4469-b387-635dff51e7e5.png)

#### Snowflake
![Implementação snowflake](snowflake/evidencias/img/)

---

## 3. Performance

| Critério | Databricks | Snowflake |
|---|---|---|
| Modelo de computação | Serverless Compute, elástico, provisionado sob demanda só durante a execução do notebook | Warehouse `WH_DI_P_PIPELINE`, tamanho `XSMALL`, auto-suspend em 60s |
| Adequação ao volume do projeto | Overkill para o volume atual. Todas as Copas do Mundo entre 1930 e 2026 somam 1.067 partidas, não dezenas de milhares. Spark compensa esse overhead em volumes bem maiores | Mais que suficiente para o volume atual. O warehouse `XSMALL` já sobra em capacidade de processamento |
| Tempo de execução Medallion | Bronze 1m 9s, Silver 38s e Gold 32s, totalizando 2m 22s | Pendente |
| Custo de "cold start" | Não há cold start manual. O Serverless Compute já inclui o provisionamento elástico dentro do tempo medido acima, sem cluster para ligar antes da execução | O warehouse `XSMALL` sobe em segundos e é cobrado a partir do primeiro segundo de uso |

### Evidencias

As evidencias completas estão disponiveis na pasta dos respectivos pipelines, dentro de `evidencias`.

#### Databricks
![Implementação databricks](databricks/evidencias/img/c32533c1-d495-4056-8192-20642d38827f.png)

#### Snowflake
![Implementação snowflake](snowflake/evidencias/img/)

### Projeção de escala (de minutos para horas por semana)

O volume atual do projeto é pequeno. O pipeline do Databricks roda em 2 minutos e 22 segundos por semana. O tempo do Snowflake ainda não foi medido, mas deve ficar na mesma ordem de grandeza. Para discutir o que aconteceria se o projeto crescesse, projetamos um cenário hipotético de algumas horas de processamento por semana, por exemplo se o pipeline passasse a rodar diariamente durante o mês de Copa ou passasse a incorporar mais ligas e temporadas.

| Cenário | Databricks | Snowflake |
|---|---|---|
| Atual (~2m22s por semana) | O Serverless Compute provisiona e processa dentro dos 2m22s medidos, sem cluster para gerenciar e sem tempo de boot separado do processamento | O warehouse `XSMALL` sobe em segundos, processa e entra em auto-suspend em 60s. Praticamente todo o tempo cobrado é processamento de fato |
| Projetado (algumas horas/semana) | O Serverless escala elasticamente com a carga. O tempo tende a crescer de forma linear com o volume, sem degrau de redimensionamento manual | Rodando o mesmo warehouse `XSMALL` por horas, o tempo escala de forma linear. Se o volume exigir mais velocidade, o warehouse precisa ser redimensionado manualmente |

O Databricks Serverless escala automaticamente, sem intervenção manual. O Snowflake exige redimensionar o warehouse manualmente (de `XSMALL` para `SMALL` e assim por diante) quando a velocidade não é suficiente. Isso torna a operação mais simples no Databricks, mas o Snowflake oferece um controle de custo mais previsível justamente por não escalar sozinho.

---

## 4. Governança

| Critério | Databricks | Snowflake |
|---|---|---|
| Catálogo/organização | Unity Catalog (`workspace.bronze/silver/gold`) | Banco `DI_P_MEDALLION` com schemas `BRONZE/SILVER/GOLD/QC/PIPE` |
| Linhagem (lineage) | Unity Catalog rastreia automaticamente | Rastreada manualmente via `SOURCE_FILE`, `_RUN_ID`, `QC.PIPELINE_RUNS` |
| Controle de acesso | Unity Catalog RBAC | RBAC nativo do Snowflake (roles/grants). Não implementado explicitamente nos scripts do trabalho |
| Qualidade de dados | Quarentena em tabelas Delta separadas | Quarentena em tabelas + `QC.CHECK_RESULTS`/`QC.PIPELINE_RUNS` + testes dbt |
| Dados sensíveis | N/A (base pública, sem PII**) | N/A (base pública, sem PII**) |

> ** PII: Personally Identifiable Information  
### Frequência de atualização

Semanal, toda segunda-feira às 06h, na temporada corrente (agosto a maio). Mensal, no dia 1 às 06h, apenas verificação, no recesso (junho a julho). Correções sob demanda. Cadência implementada em `06_pipeline_procedures_and_schedule.sql` (`PIPE.TASK_WORLDCUP_WEEKLY`) e documentada em `ARCHITECTURE.md`.

### Retenção (hot/cold por camada)

Política geral: manter tudo, já que o custo de storage é baixo (a base cabe em dezenas de MB) e não há obrigação legal de expurgo, pois não há dado pessoal. A diferença é apenas entre armazenamento hot e cold conforme a idade do dado:

| Camada | O que guardar | Retenção | Depois disso |
|---|---|---|---|
| Bronze | Arquivos brutos (JSON/TXT) | Indefinido | Cold storage após 24 meses sem acesso |
| Silver | Dado limpo consolidado | Indefinido | N/A |
| Silver | Versões intermediárias de migração de schema | 90 dias | Descarte |
| Gold | Versão atual das métricas | Indefinido | N/A |
| Gold | Versões históricas (modelos anteriores) | 12 meses | Cold storage |

Essa política é relevante para a seção de custo: como a política é manter tudo e o volume é pequeno, o custo de storage tende a zero nas duas plataformas. A variável de custo real do projeto é compute (DBU ou crédito), não armazenamento.

### Dados sensíveis (ausência de PII)

Mesmo sem dado sensível na base escolhida, definimos uma abordagem para o caso de haver: tokenização e mascaramento antes da persistência, com o mapeamento guardado em um cofre externo do tipo AWS Secrets Manager ou Vault, anonimização irreversível na Silver, e controle de acesso por perfil (engenheiro de dados com acesso total, analista e cientista de dados com leitura restrita ou anonimizada, consumidor de dashboard restrito à Gold). As duas plataformas têm suporte nativo equivalente:

| Mecanismo | Databricks | Snowflake |
|---|---|---|
| Mascaramento dinâmico | Unity Catalog Column Masks | Dynamic Data Masking Policies |
| Auditoria de acesso | Unity Catalog (log nativo) | Access History |

Essa abordagem de RBAC é uma decisão de design documentada, sem implementação em código no repositório atual, o que é consistente com a tabela de governança acima.

---

## 5. Custo

### Modelo de cobrança

**Databricks: DBU (Databricks Unit)**

- Cobrança por DBU consumida, multiplicada pelo preço por DBU, que varia por tipo de compute e por tier.
- O pipeline deste projeto roda em Serverless Compute, não em cluster clássico. No compute clássico, o DBU não inclui a VM, e a infraestrutura cloud é cobrada separadamente. No Serverless, o compute roda na própria conta cloud da Databricks e o custo de infraestrutura já vem embutido no preço do DBU, sem fatura separada de VM para gerenciar.
- Referência de mercado (2026, valores públicos de list price, tratados como ordem de grandeza, não como número fechado):
  * Jobs Compute clássico: aproximadamente US$ 0,15 a US$ 0,30 por DBU.
  * SQL/Data Warehousing: aproximadamente US$ 0,22 por DBU.
  * Interactive/All-Purpose (notebook manual): aproximadamente US$ 0,40 por DBU.
  * Serverless (Jobs/SQL) tende a cobrar um pouco mais por DBU que o Jobs Compute clássico, mas em troca já embute o custo de infraestrutura. Não há uma tabela pública única para o rate exato de Serverless Jobs.
- O tier Standard está sendo descontinuado em 2026, com migração para o tier Premium, que custa de 30% a 37% a mais que os valores de Standard citados acima.

**Snowflake: crédito de warehouse + storage**

- Cobrança por crédito consumido pelo warehouse, 1 crédito por hora para `XSMALL`, dobrando a cada tamanho: Small 2 créditos, Medium 4 créditos, Large 8 créditos.
- Armazenamento cobrado separadamente do compute, cerca de US$ 23 por TB por mês.
- Preço do crédito por edição (2026, list price): Standard aproximadamente US$ 2 por crédito, Enterprise aproximadamente US$ 3, Business Critical aproximadamente US$ 4.
- Warehouse do projeto: `XSMALL`, com `AUTO_SUSPEND = 60`, o que economiza créditos entre execuções, já que o pipeline roda uma vez por semana.

Os valores acima vêm de pesquisa pública de mercado, não de fatura real. Pendente: substituir por valores medidos nas duas plataformas.

### Estimativa para o cenário do grupo

A estimativa a seguir considera o volume atual do projeto, com o tempo real medido no Databricks (2 minutos e 22 segundos por semana) e o tempo do Snowflake ainda pendente, além de uma projeção de crescimento para algumas horas de processamento por semana, por exemplo se o pipeline passasse a rodar diariamente durante o mês de Copa e incorporasse mais ligas e temporadas. Em ambos os cenários, o warehouse e o compute são mantidos no menor nível disponível, `XSMALL` no Snowflake e Serverless mínimo no Databricks, sem redimensionamento.

| Item | Databricks (Serverless) | Snowflake (`XSMALL`) |
|---|---|---|
| Frequência de execução | Semanal (± diária em mês de Copa) | Semanal (± diária em mês de Copa) |
| Tempo por execução, cenário atual | 2 minutos e 22 segundos, medido na execução de 21/07/2026 | Pendente |
| Custo por execução, cenário atual | Estimado entre US$ 0,02 e US$ 0,10, aplicando as faixas de DBU acima sobre 2m22s. O consumo real de DBU ainda não foi medido | Estimado entre US$ 0,10 e US$ 0,30, fração de 1 crédito sobre poucos minutos de uso |
| Custo mensal, cenário atual (fora de Copa) | Estimado abaixo de US$ 1 por mês | Estimado entre US$ 1 e US$ 3 por mês |
| Tempo por execução, cenário projetado | 3 horas por semana (hipótese de trabalho) | 3 horas por semana (hipótese de trabalho) |
| Custo por execução, cenário projetado | Estimado entre US$ 0,45 e US$ 1,20 por semana, aplicando a faixa Serverless/Jobs sobre 3h | Estimado entre US$ 6 e US$ 12 por semana, equivalente a 3 créditos entre US$ 2 e US$ 4 |
| Custo mensal, cenário projetado (× 4,33 semanas) | Estimado entre US$ 2 e US$ 5 por mês | Estimado entre US$ 26 e US$ 52 por mês |

No volume atual, medido em minutos por semana, o custo absoluto é irrelevante nas duas plataformas, ficando abaixo de um dólar por mês. Na projeção de horas por semana essa diferença fica mais visível. Como o pipeline Databricks já roda em Serverless, a faixa de DBU aplicável é a mais barata entre as opções consideradas. Mesmo na faixa alta de DBU Serverless, em torno de US$ 0,40, o custo ainda fica abaixo de 1 crédito Snowflake, entre US$ 2 e US$ 4, por hora equivalente de processamento.

Essa comparação aplica preços de mercado sobre o tempo real medido no Databricks, e não sobre o custo real faturado. Pendente: consumo real de DBU e de crédito nas duas plataformas.

No Snowflake, a escalabilidade de custo depende do tamanho do warehouse, que dobra a cada tier. No Databricks Serverless, a escalabilidade é automática e contínua.

A frequência de execução está detalhada em `ARCHITECTURE.md`.

---

## 6. Conclusão

### TL;DR;

> Conclusão final: SNOWFLAKE

### Detalhamento

O volume de dados do projeto é pequeno: 1.067 partidas e 1.195 eventos de gol, cobrindo todas as edições da Copa do Mundo entre 1930 e 2026, sem nenhum registro rejeitado pelas regras de qualidade. Esse volume não justifica, por si só, a escolha de nenhuma das duas plataformas em termos de capacidade de processamento.

O pipeline Databricks processa esse volume em 2 minutos e 22 segundos, rodando em Serverless Compute, sem necessidade de gerenciar cluster. A mesma regra de negócio está implementada nas duas plataformas por meio de modelos dbt idênticos, o que garante que o resultado analítico, o percentual de reação por edição de Copa do Mundo, é equivalente entre Databricks e Snowflake.

Para o volume atual, as duas plataformas são tecnicamente adequadas e de baixo custo. A diferença mais relevante entre elas não está em performance ou em custo bruto, mas na operação. O Snowflake exige apenas conhecimento de SQL e não tem infraestrutura para gerenciar, o que reduz a barreira de entrada para quem não conhece Spark. O Databricks, rodando em Serverless, também elimina boa parte da complexidade operacional de cluster que tradicionalmente pesaria contra ele.

Na projeção de crescimento para algumas horas de processamento por semana, o Databricks Serverless tende a apresentar custo menor que o Snowflake no mesmo cenário, considerando os preços de mercado levantados na seção 5. Essa vantagem de custo depende da disciplina operacional de manter o pipeline como execução automatizada, e não como notebook interativo, que é cobrado a um DBU mais alto.

Diante do exposto, a recomendação do grupo para este caso é o **Snowflake**, pela simplicidade operacional de depender apenas de SQL, pela ausência de infraestrutura para gerenciar e pela previsibilidade do modelo de cobrança por warehouse. O Databricks permanece como alternativa competitiva em custo projetado de crescimento, e se torna preferível caso precisemos futuramente de processamento distribuído em maior escala ou de cargas de machine learning no mesmo ambiente.

**Itens pendentes para fechar a análise com números medidos dos dois lados**: tempo de execução real do Snowflake, volume processado por camada no Snowflake, e consumo real de DBU e de crédito nas duas plataformas.
