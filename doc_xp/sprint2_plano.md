# Sprint 2 — Validação de Qualidade e Camada Silver
**Projeto:** Análise de E-commerce — Pipeline de dados e validação de qualidade
**Autor:** Leonardo de Oliveira Paschoal
**Período:** 24/08/2026 a 04/09/2026 (10 dias úteis)
**Repositório:** https://github.com/leo-dev-07/xp-ecommerce-analytics

---

## 1. Contexto: de onde a Sprint 2 parte

A Sprint 1 entregou a infraestrutura de origem e a camada Bronze:

| Entregue na Sprint 1 | Estado |
|---|---|
| Docker Compose com Zookeeper, Kafka Broker, Schema Registry, Kafka Connect, ksqlDB, REST Proxy, Postgres e Event Generator | Funcionando |
| Event Generator populando 6 tabelas `brz_*` no Postgres a cada 60 s | Funcionando |
| Source connector JDBC Postgres → tópicos `ecommerce.brz_*` | Funcionando (`mode: bulk`) |
| Sink connector Kafka → Azure Blob (JSON, particionado por hora) | Funcionando |
| Workspace Databricks + catálogo `ecommerce` + Delta Tables na camada Bronze | Funcionando |
| Contagens comparativas entre origem e destino | Feito de forma manual/pontual |
| Notebook `2_dim_silver.ipynb` | Iniciado e incompleto |

### 1.1 Ajuste de rota registrado no relatório

O desenho original do CANVAS descrevia eventos de clique publicados no Kafka e indexados no **Elasticsearch**. Durante a Sprint 1 a arquitetura efetivamente construída foi:

```
Event Generator → PostgreSQL (bronze) → Kafka Connect (JDBC source) → Kafka
                                                                        ↓
                                             Azure Blob Storage ← Kafka Connect (sink)
                                                                        ↓
                                                     Databricks — Bronze / Silver / Gold (Delta)
```

A decisão da Sprint 2 é **assumir essa arquitetura como a arquitetura real do projeto** e atualizar o relatório, em vez de reintroduzir o Elasticsearch. A justificativa é registrada como pivô consciente:

- o problema central (validar automaticamente se o que a origem gera chega íntegro ao destino analítico) permanece **idêntico** — muda apenas o par origem/destino;
- as hipóteses H1 (perda de eventos), H2 (divergência de schema) e H3 (validação escalável e recorrente) continuam integralmente aplicáveis e agora são testáveis com dados reais do próprio pipeline;
- introduzir o Elasticsearch nesta altura consumiria a sprint em infraestrutura, sem agregar nada à validação das hipóteses.

Onde o relatório diz "Elasticsearch", passa a dizer "camada analítica no Databricks (Delta Lake), alimentada via Azure Blob". Onde diz "eventos de clique", passa a dizer "eventos de mudança de dados transacionais do e-commerce (`brz_*`)".

### 1.2 Achados técnicos da Sprint 1 que a Sprint 2 precisa tratar

Estes quatro achados saíram da leitura do próprio código e são o que dá substância à Sprint 2:

**A1 — O source connector opera em `mode: bulk`.**
Em `connectors/source/connector_brz_all_tables.json`, o conector usa `"mode": "bulk"` com `"poll.interval.ms": "60000"`. Em modo *bulk* o conector **relê a tabela inteira a cada poll** e republica todas as linhas no tópico. Isso significa que a duplicidade observada no destino **não é uma falha aleatória: é determinística e explicada pela configuração**. É a evidência mais forte disponível para H1/H2 e precisa ser quantificada, não apenas descrita.

**A2 — Os schemas da camada Bronze divergem do DDL da origem.**
Comparando `postgres/init.sql` com os `StructType` de `src/notebook/1_dim_bronze.ipynb`:

| Tabela | Colunas no Postgres (origem) | Colunas no Bronze (Databricks) | Situação |
|---|---|---|---|
| `brz_products` | product_id, product_name, brand_code, category_code, price, description, dt_update | product_id, sku, category_code, brand_code, color, size, material, weight_grams, length_cm, width_cm, height_cm, rating_count, file_name, ingest_timestamp | **Divergência grave** |
| `brz_order_items` | order_item_id, order_id, customer_id, product_id, date_code, quantity, unit_price, total_price, dt_update | dt, order_ts, customer_id, order_id, item_seq, product_id, quantity, unit_price, discount_pct, tax_amount, channel, copoun_code | **Divergência grave** |
| `brz_calendar` | date_code, full_date, year, quarter, month, month_name, day, day_of_week, day_name, is_weekend, dt_update | date, year, day_name, quarter, week_of_year | **Divergência** |
| `brz_customers` | customer_id, phone, country_code, country, state, city, dt_update | schema declarado sem `city`; leitura efetiva ignora o schema | **Divergência parcial** |
| `brz_brands` | brand_code, brand_name, category_code, dt_update | brand_code, brand_name, category_code | OK (falta `dt_update`) |
| `brz_category` | category_code, category_name, dt_update | category_code, category_name | OK (falta `dt_update`) |

Isto é **H2 confirmada com evidência documental**, antes mesmo de rodar qualquer teste.

**A3 — A camada Bronze não lê o destino do pipeline.**
O notebook Bronze lê CSVs de `/Volumes/ecommerce/source_data/raw/ecomm-raw-data/...`, enquanto o sink connector grava JSON em `kafka-data/ecommerce.brz_*/year=.../month=.../day=.../hour=...` no Azure Blob. Ou seja: o pipeline **não está fechado ponta a ponta** — a Bronze consome um dataset paralelo. Enquanto isso não for corrigido, qualquer reconciliação mede a coisa errada.

**A4 — A camada Silver está incompleta.**
`2_dim_silver.ipynb` tem funções de limpeza apenas para `brands`, uma célula com erro de sintaxe (`df = df.phone.`) e uma célula de listagem de storage accounts do Azure que não pertence à camada Silver. Além disso, o notebook contém um **Subscription ID exposto em texto claro** — precisa ir para secret scope.

---

## 2. Objetivo da Sprint 2

> Fechar o pipeline ponta a ponta (Azure Blob → Bronze → Silver) e transformar a validação de qualidade de uma checagem manual em um **processo executável e reprodutível**, produzindo métricas quantificadas de perda (H1) e de divergência de schema (H2) para pelo menos 95 % das tabelas mapeadas.

Ligação com o Objetivo SMART do CANVAS: esta sprint entrega as camadas Bronze/Silver e a comparação origem × destino; a cobertura de ≥ 95 % dos tipos de evento (aqui: 6 de 6 tabelas = 100 %) é medida ao final da sprint pela tabela `gold.dq_coverage`.

### 2.1 Hipóteses endereçadas

| Hipótese | Como a Sprint 2 a testa | Métrica de decisão |
|---|---|---|
| **H1** — parte dos registros publicados no Kafka não chega íntegra ao destino | Reconciliação de contagens Postgres × Kafka × Bronze × Silver por tabela e por janela de tempo | `taxa_perda = 1 − (distintos_destino / distintos_origem)`; H1 confirmada se > 0 em qualquer tabela |
| **H2** — existe divergência de schema entre o contrato da origem e o que está armazenado no destino | Validação programática de cada tabela contra o contrato derivado do DDL | nº de colunas ausentes, extras e com tipo divergente por tabela; H2 confirmada se > 0 |
| **H3** — uma arquitetura medalhão permite fazer isso de forma escalável e recorrente | Parcialmente: a sprint entrega o validador executável; o agendamento fica para a Sprint 3 | validador roda ponta a ponta em < 10 min sem intervenção manual |

---

## 3. Backlog da Sprint 2

Sete cards, sendo o C7 *stretch*. Cada card tem um artefato de evidência associado — o número de artefatos deve bater com o número de requisitos planejados, conforme exige o template do relatório.

### C1 — Fechar a ingestão Bronze a partir do Azure Blob
**Requisito:** a camada Bronze passa a ler o JSON produzido pelo sink connector (`kafka-data/ecommerce.brz_*`), com Auto Loader / Structured Streaming e checkpoint, em vez de CSVs avulsos em Volume.

**Tarefas**
1. Configurar acesso do Databricks ao container Azure (secret scope, sem chave em texto claro no notebook).
2. Criar External Location / Volume apontando para `kafka-data/`.
3. Reescrever a ingestão das 6 tabelas com `cloudFiles` (`format = json`), `mergeSchema` e checkpoint por tabela.
4. Adicionar colunas de linhagem: `_source_file`, `_ingested_at`, `_kafka_topic`, `_blob_partition_hour`.

**Critério de aceite:** as 6 tabelas `bronze.brz_*` são materializadas exclusivamente a partir do Azure Blob; reexecutar o notebook não duplica linhas (checkpoint funcionando); `SELECT count(*)` na Bronze > 0 para todas.

**Evidência:** notebook `1_dim_bronze.ipynb` atualizado + print do Catalog Explorer com as 6 Delta Tables e suas contagens.

---

### C2 — Formalizar os contratos de dados
**Requisito:** o contrato de cada tabela deixa de ser implícito (espalhado em `StructType` dentro de notebooks) e passa a ser declarado em um único lugar versionado, derivado do DDL do Postgres, que é a fonte da verdade.

**Tarefas**
1. Criar `src/quality/contracts.py` com o contrato das 6 tabelas: colunas, tipo canônico, nulidade, chave de negócio, coluna de versionamento (`dt_update`) e regras de domínio.
2. Exportar os contratos em JSON (`docs/contracts/*.json`) para anexar ao relatório.
3. Documentar a política de evolução de schema (o que é mudança compatível e o que é *breaking*).

**Critério de aceite:** as 6 tabelas têm contrato declarado; o contrato é importável e serializável; `python -m src.quality.contracts --export` gera os JSON sem erro.

**Evidência:** `src/quality/contracts.py` + os 6 arquivos JSON de contrato.

---

### C3 — Validador de schema e tabela de quarentena
**Requisito:** validar programaticamente cada DataFrame contra o contrato e separar o que não conforma, em vez de deixar passar silenciosamente.

**Tarefas**
1. Criar `src/quality/validation.py` com as checagens: colunas ausentes, colunas extras, tipo divergente, nulos em coluna obrigatória, violação de chave (duplicidade de PK), violação de domínio.
2. Cada registro reprovado vai para `silver.qtn_<tabela>` com o motivo (`_dq_failed_rules`) e o timestamp da validação.
3. Cada execução grava um resumo em `gold.dq_schema_violations`.

**Critério de aceite:** rodar o validador contra a Bronze atual **reproduz e quantifica** as divergências do achado A2; nenhum registro é descartado sem rastro (aprovado → Silver, reprovado → quarentena, soma = total).

**Evidência:** `src/quality/validation.py` + print da tabela `gold.dq_schema_violations` com as divergências por tabela.

---

### C4 — Camada Silver completa com deduplicação
**Requisito:** as 6 tabelas Silver, limpas, tipadas, deduplicadas por chave de negócio e sem os problemas herdados do notebook atual.

**Tarefas**
1. Deduplicar por chave de negócio mantendo a versão mais recente (`row_number() over (partition by pk order by dt_update desc, _ingested_at desc)`) — trata diretamente a duplicidade determinística causada pelo `mode: bulk` (achado A1).
2. Padronizar: `trim` em textos, normalização de `category_code` (anomalias `GROCERY→GRCY`, `BOOKS→BKS`, `TOYS→TOY` já mapeadas na Sprint 1), *cast* de tipos conforme contrato, normalização de telefone e de `country_code`.
3. Remover a célula de listagem de storage accounts do notebook Silver e mover o Subscription ID para secret scope; corrigir a célula quebrada de `customers`.
4. Registrar, por tabela, quantas linhas entraram, quantas foram descartadas por dedup e quantas foram para quarentena.

**Critério de aceite:** `silver.slv_*` existe para as 6 tabelas; PK única em todas (`count(*) = count(distinct pk)`); nenhum segredo em texto claro no notebook; notebook executa de ponta a ponta sem erro.

**Evidência:** `2_dim_silver.ipynb` completo + print do resultado do teste de unicidade de PK nas 6 tabelas.

---

### C5 — Reconciliação Postgres × Kafka × Bronze × Silver
**Requisito:** medir H1. Contagens comparativas automatizadas nas quatro pontas, com o resultado persistido — substituindo a checagem manual da Sprint 1.

**Tarefas**
1. Criar `scripts/reconcile_sources.py`: conta linhas no Postgres (`SELECT count(*)` e `count(distinct pk)`), conta offsets/mensagens por tópico no Kafka, conta linhas e PKs distintas na Bronze e na Silver.
2. Calcular por tabela: `perda_absoluta`, `taxa_perda`, `taxa_duplicidade`, `defasagem_temporal` (max `dt_update` origem vs. destino).
3. Gravar o resultado em JSON + CSV e carregá-lo em `gold.dq_reconciliation`.
4. Rodar em pelo menos **3 janelas de tempo distintas** (uma logo após o start, uma após ~1 h e uma após ~4 h de geração) para responder à lição aprendida da Sprint 1 sobre volume insuficiente.

**Critério de aceite:** relatório de reconciliação gerado para as 6 tabelas nas 3 janelas; H1 declarada confirmada ou refutada **com número**, não com impressão.

**Evidência:** `scripts/reconcile_sources.py` + relatório `docs/evidencias/reconciliation_report.csv` + print do gráfico de taxa de perda por tabela.

---

### C6 — Camada Gold de qualidade e cobertura
**Requisito:** consolidar as métricas em tabelas Gold consultáveis e provar a cobertura de validação exigida pelo Objetivo SMART.

**Tarefas**
1. Criar `3_gold_data_quality.ipynb` materializando `gold.dq_metrics` (uma linha por tabela × execução × dimensão de qualidade), `gold.dq_reconciliation` e `gold.dq_coverage`.
2. Dimensões medidas: completude, unicidade, conformidade de schema, validade de domínio, atualidade (*freshness*).
3. Definir *thresholds* e uma coluna `status` (`OK` / `WARN` / `FAIL`) por métrica.
4. Consultas SQL prontas para o dashboard (a construção do dashboard fica na Sprint 3).

**Critério de aceite:** `gold.dq_coverage` mostra ≥ 95 % das tabelas mapeadas cobertas por pelo menos 4 das 5 dimensões; todas as métricas têm *threshold* e status.

**Evidência:** `3_gold_data_quality.ipynb` + print de `SELECT * FROM ecommerce.gold.dq_metrics`.

---

### C7 (stretch) — Corrigir o `mode: bulk` e medir o antes/depois
**Requisito:** trocar o source connector de `bulk` para `timestamp` usando `dt_update` como coluna incremental, e medir o efeito na taxa de duplicidade.

**Tarefas**
1. Criar `connectors/source/connector_brz_all_tables_incremental.json` com `"mode": "timestamp"`, `"timestamp.column.name": "dt_update"`.
2. Rodar a reconciliação (C5) antes e depois da troca.
3. Documentar a comparação.

**Critério de aceite:** queda mensurável da taxa de duplicidade entre as duas configurações.

**Evidência:** as duas configurações de conector + tabela comparativa antes/depois.

> Se a sprint apertar, C7 é o primeiro a ser cortado — sem ele a Sprint 2 ainda entrega o objetivo. Mas ele é o card com melhor relação impacto/esforço para o relatório, porque mostra diagnóstico → correção → medição.

---

## 4. Cronograma

| Dia | Data | Foco |
|---|---|---|
| 1 | seg 24/08 | C1 — acesso Azure, secret scope, External Location |
| 2 | ter 25/08 | C1 — Auto Loader das 6 tabelas, checkpoints |
| 3 | qua 26/08 | C2 — contratos + export JSON |
| 4 | qui 27/08 | C3 — validador de schema |
| 5 | sex 28/08 | C3 — quarentena + `gold.dq_schema_violations`; **1ª janela de reconciliação** |
| 6 | seg 31/08 | C4 — Silver: dedup e limpeza (brands, category, customers) |
| 7 | ter 01/09 | C4 — Silver: calendar, products, order_items + teste de PK |
| 8 | qua 02/09 | C5 — script de reconciliação; **2ª e 3ª janelas** |
| 9 | qui 03/09 | C6 — Gold de qualidade e cobertura |
| 10 | sex 04/09 | C7 (se houver folga), coleta de evidências, retrospectiva, seção 2.2 do relatório |

**Cerimônias:** planning em 24/08 (manhã), review + retrospectiva em 04/09 (tarde). Board no Trello espelhando os cards C1–C7, com o link em modo *observer* para o relatório, como feito na Sprint 1.

---

## 5. Definition of Done da Sprint

Uma sprint só está encerrada quando **todos** os itens abaixo forem verdadeiros:

- [ ] Os 6 cards obrigatórios (C1–C6) estão em *Done* no Trello.
- [ ] O código está commitado e versionado no GitHub, com um commit por card.
- [ ] O pipeline roda de ponta a ponta a partir do Azure Blob, sem passo manual.
- [ ] As 6 tabelas Silver existem, com PK única comprovada por teste.
- [ ] `gold.dq_metrics`, `gold.dq_reconciliation` e `gold.dq_coverage` estão populadas.
- [ ] H1 e H2 têm veredito quantificado (confirmada/refutada, com o número que sustenta).
- [ ] Nenhum segredo em texto claro no repositório.
- [ ] Evidências (prints, CSVs, links) coletadas na pasta `docs/evidencias/`.
- [ ] Seção 2.2 do relatório preenchida com um artefato por requisito.

---

## 6. Riscos da sprint

| Risco | Prob. | Impacto | Mitigação |
|---|---|---|---|
| Acesso do Databricks ao Azure Blob falhar (permissão/SAS) | Média | Alto — bloqueia C1 e tudo depois | Testar no **dia 1**; plano B: baixar uma amostra do blob para Volume e seguir com o desenvolvimento, corrigindo o acesso em paralelo |
| Volume de dados ainda pequeno demais para conclusões sobre H1 (lição da Sprint 1) | Média | Médio | Deixar o Event Generator rodando continuamente desde 24/08 e usar 3 janelas de tempo, sendo a última com ≥ 4 h de acúmulo |
| Databricks Free/Trial com limite de compute | Média | Médio | Cluster pequeno, processamento em lote por tabela, evitar `display()` em DataFrames grandes |
| Divergências de schema (A2) serem tantas que a Silver vire refatoração completa | Alta | Médio | A Silver mapeia o contrato para o que **existe** na Bronze; o que não existir vira violação registrada, não bloqueio de execução |
| Diferença de granularidade de timestamp entre origem e destino (lição da Sprint 1) | Alta | Baixo | Padronizar tudo em epoch millis (`dt_update`) e arredondar para minuto nas comparações |

---

## 7. Mapa card → evidência → seção do relatório

| Card | Artefato | Onde entra no relatório |
|---|---|---|
| C1 | `1_dim_bronze.ipynb` + print do Catalog | 2.2.1 — evidência do requisito 1 |
| C2 | `src/quality/contracts.py` + JSONs | 2.2.1 — evidência do requisito 2 |
| C3 | `src/quality/validation.py` + print de `dq_schema_violations` | 2.2.1 — evidência do requisito 3 |
| C4 | `2_dim_silver.ipynb` + print do teste de PK | 2.2.1 — evidência do requisito 4 |
| C5 | `scripts/reconcile_sources.py` + `reconciliation_report.csv` | 2.2.1 — evidência do requisito 5 |
| C6 | `3_gold_data_quality.ipynb` + print de `dq_metrics` | 2.2.1 — evidência dos resultados |
| C7 | configs de conector + tabela antes/depois | 2.2.1 — evidência dos resultados (complementar) |
| Todos | veredito de H1 e H2 | 2.2.2 — retrospectiva |
