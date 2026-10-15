# Olist · Data warehouse com dbt e DuckDB

## Problema
Como transformar tabelas transacionais de um marketplace (pedidos, itens, pagamentos, avaliações) em indicadores de
receita, entrega e recompra, de forma que cada número seja auditável até a tabela de origem? Este projeto usa dbt para
transformar o dado em código versionado, com dependências explícitas (`ref`/`source`), testes e documentação.

## Dados
- **Tipo: SINTÉTICO** (rótulo `synthetic`). O Brazilian E-Commerce Public Dataset da Olist (licença CC BY-NC-SA 4.0)
  só é distribuído no Kaggle, que exige credencial. Este repositório **não contém dados reais da Olist**.
- O gerador `olist_warehouse.make_sample` produz as 8 tabelas com o **mesmo schema** do dataset original (incluindo o
  nome de coluna `product_name_lenght`, que o staging corrige), com seed fixa (42) e distribuições plausíveis: status de
  pedido, atraso de entrega, parcelas, notas 1 a 5 correlacionadas com atraso e com vendedor/categoria, nomes de categoria
  reais em português com tradução para inglês.
- Volume gerado: 20.000 pedidos, 23.215 itens, 20.589 pagamentos, 19.577 avaliações, 7.200 produtos, 900 vendedores.
- Para rodar com o dado real: `kaggle datasets download -d olistbr/brazilian-ecommerce`, descompactar os CSVs em
  `data/raw/` com os mesmos nomes e executar `python -m olist_warehouse.pipeline --skip-sample`.
  **Esse caminho não foi executado neste projeto.**
- Fonte e licença do original: Olist, Brazilian E-Commerce Public Dataset (Kaggle), CC BY-NC-SA 4.0. Uso não comercial, com atribuição.

## Abordagem
1. `load_raw`: carrega os CSVs como texto no schema `raw` do DuckDB (etapa EL, sem tipagem).
2. **staging** (`stg_*`, views): tipagem, normalização de texto e UF, CEP com 5 dígitos, correção do nome de coluna.
3. **intermediate**: agregações reutilizáveis por pedido (itens, pagamentos, avaliação mais recente, métricas de entrega).
4. **marts**: `dim_customers` (grão `customer_unique_id`), `dim_products`, `dim_sellers`, `dim_date`, `fct_orders` (1 linha por pedido), `fct_order_items`.
5. **gold** (KPIs): `kpi_overview`, `kpi_monthly_revenue`, `kpi_delivery_sla` (somas e contagens reagregáveis),
   `kpi_category_reviews`, `kpi_top_sellers`, `kpi_repurchase_cohort` (retenção e recompra acumulada por coorte).
6. Testes: 139 testes dbt, incluindo `unique`, `not_null`, `relationships`, `accepted_values`, dois testes genéricos próprios
   (`non_negative`, `column_not_before`), um genérico de unicidade de combinação (`unique_combination`) e 6 testes singulares.
   Um deles (`assert_delivered_orders_have_delivery_date`) tem severidade `warn`.
7. `export_gold`: copia marts e KPIs para parquet zstd em `data/`, gera `lineage.json` (nós e arestas do `manifest.json`),
   `dbt_run_results.json` (resumo enxuto dos testes e modelos) e `data_dictionary.json`.

Diagrama de camadas: `source → staging → intermediate → marts → gold`.

## Resultados
Números de uma execução real de `dbt build` (dbt-core 1.12.6, dbt-duckdb 1.11.0, DuckDB 1.5.6, dados sintéticos):

| Métrica (dados sintéticos) | Valor |
|---|---|
| Pedidos totais / válidos (sem cancelados e indisponíveis) | 20.000 / 19.776 |
| Receita (itens + frete), pedidos válidos | R$ 2.630.612,10 |
| Ticket médio (pedidos com itens) | R$ 133,02 |
| % de pedidos entregues com atraso | 7,55% |
| Prazo médio de entrega | 14,3 dias |
| Nota média | 4,25 |
| Clientes com mais de um pedido | 4,63% |

`dbt build`: **PASS=163, WARN=1, ERROR=0** de 164 nós (25 modelos construídos, 138 testes passaram, 1 teste em aviso).
O aviso é intencional: o gerador reproduz de propósito um defeito do dado real (6 pedidos marcados como entregues sem data de entrega).
Execução do `dbt build` em cerca de 3 s.

Os números ilustram o pipeline. Não representam o desempenho do Olist real.

## Como rodar
```bash
# venv pesado deste projeto (fora do app)
python3 -m venv ~/.venvs/de-olist && ~/.venvs/de-olist/bin/pip install -r projects/olist-dbt-warehouse/requirements.txt
# (o Makefile usa esse venv por padrão; para outro, rode com PY_OLIST=/caminho/bin/python)
make data-olist-dbt-warehouse        # gera CSVs sintéticos, carrega raw, dbt build, exporta gold
make test                            # testes do gerador, do export e do load (pytest)
```
Execução manual: `PYTHONPATH=projects/olist-dbt-warehouse/src python -m olist_warehouse.pipeline`.
O `dbt` roda em `projects/olist-dbt-warehouse/dbt/` com `DBT_PROFILES_DIR` apontando para essa pasta.

## Limitações
- **Dados sintéticos.** Os KPIs não representam o comportamento real de clientes e vendedores da Olist.
  O gerador replica o schema, não a distribuição exata do original.
- **Caminho com o dado real não executado** (exige credencial do Kaggle).
- **Docker não foi usado.** A execução foi nativa (macOS, Python 3.11, dbt-duckdb). Não há Dockerfile neste projeto.
- Um cliente pode ter mais de um `customer_id` (um por pedido), conforme o dado original. A dimensão usa `customer_unique_id`.
- O teste `accepted_values` de UF usa a lista de UFs brasileiras fixada no `schema.yml`, não um dado de referência externo.
- `dbt-utils` não é usado: o teste de unicidade de combinação e o `dim_date` são implementados no próprio projeto para evitar dependências extras.
- A cohort de recompra considera apenas pedidos válidos e mede recompra por mês, não por janela de dias.
