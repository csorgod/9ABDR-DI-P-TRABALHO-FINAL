# Divisão do Trabalho - Databricks vs. Snowflake

| Membro   | Responsável(is)            | Deadline   |
| -------- | --------------------------- | ---------- |
| Membro 1 | Guilherme Csorgo            | 10/07/2026 |
| Membro 2 | Ludmilla e Thiago Guilherme | 17/07/2026 |
| Membro 3 | Ayrton                      | 17/07/2026 |
| Membro 4 | Guilherme Csorgo            | 24/07/2026 |
| Membro 5 | Karen                       | 28/07/2026 |
| Membro 6 | Karen                       | 01/08/2026 |

---

## Membro 1: Base de dados e fundação do projeto

**Responsável:** Guilherme Csorgo
**Deadline:** 10/07/2026

- Escolher a base pública e justificar (problema de negócio, público-alvo, confiabilidade da fonte, volume suficiente).
- Documentar: link da fonte, formato original, volume (registros/tamanho), dicionário de dados completo (colunas e significado).
- Esse material vira a **"introdução"** do relatório final e é a base que os membros 2 e 3 vão usar para construir os pipelines — então esse membro entrega primeiro.

---

## Membro 2: Pipeline completo no Databricks

**Responsáveis:** Ludmilla e Thiago Guilherme
**Deadline:** 17/07/2026

- Construir o fluxo Bronze → Silver → Gold no Databricks usando a base definida pelo Membro 1.
- Implementar os quality checks e documentar o que acontece quando um falha.
- Rodar a query final na camada Gold com resultado de negócio.
- Capturar prints de cada etapa (ou vídeo curto) como evidência.

---

## Membro 3: Pipeline completo no Snowflake

**Responsável:** Ayrton
**Deadline:** 17/07/2026

- Mesma coisa que o Membro 2, mas no Snowflake: mesma base, mesma arquitetura Medallion, mesmos quality checks, mesma query de negócio na Gold.
- Evidências equivalentes (prints/vídeo).
- **Importante:** esse membro e o Membro 2 devem alinhar previamente os critérios de quality check e a estrutura das camadas, para que a comparação do item seguinte seja justa (mesma régua nas duas plataformas).

---

## Membro 4: Comparação e conclusão

**Responsável:** Csorgo
**Deadline:** 24/07/2026

- Junta as evidências dos Membros 2 e 3 e monta a comparação lado a lado nas 5 dimensões: facilidade de uso, ingestão/transformação, performance, governança, custo.
- Pesquisa e detalha o modelo de cobrança de cada plataforma (DBU no Databricks; crédito de warehouse + storage no Snowflake) e monta a estimativa de custo para o cenário do grupo.
- Escreve a conclusão fundamentada: qual plataforma o grupo escolheria para este caso e por quê.

---

## Membro 5: Governança, sustentabilidade e diagrama de arquitetura + Pitch

**Responsável:** Karen
**Deadline:** 28/07/2026

- Define e justifica a frequência de atualização do pipeline (diária, semanal, anual — de acordo com a natureza da base escolhida pelo Membro 1).
- Define política de expurgo/retenção (o que mantém, o que arquiva, por quanto tempo).
- Descreve como trataria dados sensíveis (mascaramento, anonimização, controle de acesso), mesmo que a base escolhida não tenha esse tipo de dado.
- Monta o diagrama de arquitetura (Excalidraw, draw.io, PowerPoint etc., **nunca mermaid**) mostrando Bronze → Silver → Gold, tecnologias de cada etapa, onde ficam os quality checks, frequência de execução, e como as duas plataformas implementam essa arquitetura.
