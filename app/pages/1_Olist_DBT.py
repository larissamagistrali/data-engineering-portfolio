"""Página 1: data warehouse do Olist com dbt + DuckDB (staging -> intermediate -> marts -> KPIs)."""
import _bootstrap  # noqa: F401
import _helpers as h
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from shared import data, theme, ui

PROJECT = "olist-dbt-warehouse"
LAYERS = ["source", "staging", "intermediate", "marts", "gold"]
LAYER_COLORS = {"source": theme.INK_MUTED, **theme.color_map(LAYERS[1:])}


def path(name: str) -> str:
    return str(data.data_path(PROJECT, name))


ui.setup_page("Olist · dbt", "🧱")
ui.header(
    "Data warehouse do Olist com dbt",
    "Do dado bruto aos KPIs: staging, intermediate, marts e testes, tudo versionado e testado.",
    tags=["dbt-core", "dbt-duckdb", "DuckDB", "SQL", "testes de dados"],
    code_path="projects/olist-dbt-warehouse",
)
h.pipeline_note()

problema, dados, abordagem, resultado, demo = ui.tabs()

required = [data.data_path(PROJECT, f) for f in (
    "kpi_overview.parquet", "kpi_monthly_revenue.parquet", "kpi_delivery_sla.parquet", "kpi_category_reviews.parquet",
    "kpi_top_sellers.parquet", "kpi_repurchase_cohort.parquet", "fct_orders.parquet", "lineage.json",
    "dbt_run_results.json", "data_dictionary.json", "export_meta.json")]
if h.missing_data(required, "Rode `make data-olist-dbt-warehouse` para gerar a camada gold."):
    st.stop()

overview = data.read_parquet(path("kpi_overview.parquet")).iloc[0]
lineage = data.read_json(path("lineage.json"))
run = data.read_json(path("dbt_run_results.json"))
dictionary = data.read_json(path("data_dictionary.json"))
meta = data.read_json(path("export_meta.json"))

with problema:
    st.markdown(
        """
**Pergunta de negócio:** como transformar tabelas transacionais de um marketplace (pedidos, itens, pagamentos,
avaliações) em indicadores confiáveis de receita, entrega e recompra, de forma que qualquer pessoa consiga
auditar de onde cada número veio?

**Por que dbt:** a transformação vira código versionado, com dependências explícitas (`ref`/`source`), testes
automáticos e documentação gerada. Cada KPI é uma consulta SQL revisável, não uma planilha.

**O que o projeto entrega**
- Camadas `staging` (limpeza e tipagem), `intermediate` (regras de negócio reutilizáveis) e `marts`
  (modelo dimensional: `dim_customers`, `dim_products`, `dim_sellers`, `dim_date`, `fct_orders`, `fct_order_items`).
- Camada gold de KPIs: receita mensal, ticket médio, SLA de entrega, nota por categoria, ranking de vendedores e coorte de recompra.
- 139 testes de dados (unique, not_null, relationships, accepted_values, testes genéricos e singulares próprios).
"""
    )

with dados:
    ui.data_notice(
        "synthetic",
        "gerador determinístico `olist_warehouse.make_sample` (seed 42)",
        "O Olist original (Brazilian E-Commerce Public Dataset, CC BY-NC-SA 4.0) só é distribuído no Kaggle, que exige "
        "credencial. Os dados aqui são SINTÉTICOS, com o schema real do Olist (mesmas tabelas e colunas). "
        "Os números mostrados ilustram o pipeline, não o desempenho do Olist real.",
    )
    st.markdown(
        """
Tabelas brutas (carregadas como texto no schema `raw` e tipadas no dbt): `customers`, `orders`, `order_items`,
`order_payments`, `order_reviews`, `products`, `sellers` e `product_category_name_translation`.

Para rodar com o dado real, baixe o dataset do Kaggle (`kaggle datasets download -d olistbr/brazilian-ecommerce`)
e coloque os CSVs em `projects/olist-dbt-warehouse/data/raw/` com os mesmos nomes. Esse caminho **não foi executado** neste projeto.
"""
    )
    rows = pd.DataFrame({"tabela exportada": list(meta["rows"]), "linhas": list(meta["rows"].values())})
    h.show_table(rows)
    st.caption(f"Camada gold exportada em {meta['exported_at']} (UTC).")

with abordagem:
    st.subheader("Linhagem do dbt")
    st.caption("Gerada a partir do `manifest.json` do `dbt build` real (nós e arestas). Selecione um modelo para destacar o que ele consome e quem o consome.")
    nodes = {n["id"]: n for n in lineage["nodes"]}
    names = {n["name"]: n["id"] for n in lineage["nodes"]}
    focus = st.selectbox("Destacar modelo", ["(todos)"] + [n for n in names if not n.startswith("olist.")], key="olist_focus")

    parents: dict[str, set[str]] = {}
    children: dict[str, set[str]] = {}
    for e in lineage["edges"]:
        parents.setdefault(e["target"], set()).add(e["source"])
        children.setdefault(e["source"], set()).add(e["target"])

    def reach(start: str, graph: dict[str, set[str]]) -> set[str]:
        seen, stack = set(), [start]
        while stack:
            for nxt in graph.get(stack.pop(), ()):
                if nxt not in seen:
                    seen.add(nxt)
                    stack.append(nxt)
        return seen

    active = set(nodes)
    if focus != "(todos)":
        fid = names[focus]
        active = {fid} | reach(fid, parents) | reach(fid, children)

    dot = ["digraph G {", 'rankdir=LR; bgcolor="transparent"; nodesep=0.12; ranksep=0.9;',
           f'node [shape=box, style="rounded,filled", fontname="Helvetica", fontsize=11, margin="0.12,0.05"];',
           f'edge [color="{theme.BASELINE}", arrowsize=0.6];']
    for layer in LAYERS:
        members = [n for n in lineage["nodes"] if n["layer"] == layer]
        dot.append(f'subgraph "cluster_{layer}" {{ label="{layer}"; fontname="Helvetica"; fontsize=12; '
                   f'fontcolor="{theme.INK_SECONDARY}"; color="{theme.GRID}"; style="rounded";')
        for n in members:
            on = n["id"] in active
            fill = LAYER_COLORS[layer] if on else theme.GRID
            font = ("#0b0b0b" if layer == "gold" else "#ffffff") if on else theme.INK_MUTED
            dot.append(f'"{n["id"]}" [label="{n["name"]}", fillcolor="{fill}", fontcolor="{font}", color="{fill}"];')
        dot.append("}")
    for e in lineage["edges"]:
        if e["source"] in active and e["target"] in active:
            dot.append(f'"{e["source"]}" -> "{e["target"]}" [color="{theme.INK_SECONDARY}"];')
    dot.append("}")
    st.graphviz_chart("\n".join(dot), width="stretch")
    st.caption("Camadas (nome no contêiner): " + " → ".join(LAYERS) + f" · {len(lineage['nodes'])} nós · {len(lineage['edges'])} arestas")

    st.subheader("Dicionário de dados")
    model_names = [m["model"] for m in dictionary]
    chosen = st.selectbox("Modelo", model_names, index=model_names.index("fct_orders") if "fct_orders" in model_names else 0, key="olist_dict")
    mdl = next(m for m in dictionary if m["model"] == chosen)
    st.markdown(f"**{mdl['model']}** · camada `{mdl['layer']}` — {mdl['description']}")
    cols = pd.DataFrame(mdl["documented_columns"]).rename(columns={"column": "coluna", "type": "tipo", "description": "descrição"})
    if cols.empty:
        cols = pd.DataFrame(mdl["all_columns"]).rename(columns={"column": "coluna", "type": "tipo"})
    h.show_table(cols)
    st.caption("Colunas sem descrição documentada no `schema.yml` não aparecem aqui; o tipo vem do `catalog.json` (`dbt docs generate`).")

with resultado:
    st.subheader("KPIs da camada gold")
    ui.metric_row([
        ("Pedidos válidos", h.fmt_int(overview["orders_valid"]), "Exclui cancelados e indisponíveis."),
        ("Receita (itens + frete)", h.fmt_brl(overview["revenue_total"])),
        ("Ticket médio", h.fmt_brl(overview["avg_ticket"]), "Receita / pedidos com itens."),
        ("Pedidos atrasados", f"{overview['pct_late']:.1f}%".replace(".", ","), "Entrega posterior à data estimada."),
        ("Nota média", f"{overview['avg_review_score']:.2f}".replace(".", ","), "Avaliação de 1 a 5."),
        ("Clientes recorrentes", f"{overview['repeat_customer_pct']:.1f}%".replace(".", ","), "Mais de um pedido."),
    ])
    st.caption("Dados sintéticos: os valores ilustram o pipeline e não representam o Olist real.")

    monthly = data.read_parquet(path("kpi_monthly_revenue.parquet"))
    c1, c2 = st.columns(2)
    fig = go.Figure(go.Bar(x=monthly["month"], y=monthly["revenue_total"].astype(float), marker_color=theme.CATEGORICAL[0],
                           hovertemplate="%{x|%b/%Y}: R$ %{y:,.0f}<extra></extra>"))
    fig.update_layout(title="Receita mensal (R$)", yaxis_title=None, xaxis_title=None)
    with c1:
        ui.plot(fig, key="olist_rev")
    fig = go.Figure(go.Scatter(x=monthly["month"], y=monthly["avg_ticket"], mode="lines+markers", line_color=theme.CATEGORICAL[0],
                               hovertemplate="%{x|%b/%Y}: R$ %{y:,.2f}<extra></extra>"))
    fig.update_layout(title="Ticket médio (R$)", yaxis_title=None, xaxis_title=None)
    with c2:
        ui.plot(fig, key="olist_ticket")

    sla = data.read_parquet(path("kpi_delivery_sla.parquet"))
    by_state = (sla.groupby("customer_state")[["delivered_orders", "late_orders", "delivery_days_sum"]].sum()
                .assign(pct_late=lambda d: 100 * d["late_orders"] / d["delivered_orders"],
                        avg_days=lambda d: d["delivery_days_sum"] / d["delivered_orders"])
                .query("delivered_orders >= 100").sort_values("pct_late").reset_index())
    cats = data.read_parquet(path("kpi_category_reviews.parquet")).query("items_sold >= 200").sort_values("avg_review_score")
    c1, c2 = st.columns(2)
    fig = go.Figure(go.Bar(y=by_state["customer_state"], x=by_state["pct_late"], orientation="h", marker_color=theme.CATEGORICAL[1],
                           customdata=by_state[["delivered_orders", "avg_days"]],
                           hovertemplate="%{y}: %{x:.1f}% atrasados (%{customdata[0]} pedidos, %{customdata[1]:.1f} dias em média)<extra></extra>"))
    fig.update_layout(title="% de pedidos atrasados por UF (UFs com 100+ entregas)", xaxis_title="% atrasados", yaxis_title=None, yaxis_type="category")
    with c1:
        ui.plot(fig, key="olist_sla")
    fig = go.Figure(go.Scatter(y=cats["category_en"], x=cats["avg_review_score"], mode="markers", marker=dict(size=9, color=theme.CATEGORICAL[0]),
                               customdata=cats[["items_sold", "pct_low_score"]],
                               hovertemplate="%{y}: nota %{x:.2f} (%{customdata[0]} itens, %{customdata[1]:.0f}% notas 1-2)<extra></extra>"))
    fig.update_layout(title="Nota média por categoria (200+ itens)", xaxis=dict(title="nota média", range=[1, 5]), yaxis_title=None, yaxis_type="category")
    with c2:
        ui.plot(fig, key="olist_cat")

    cohort = data.read_parquet(path("kpi_repurchase_cohort.parquet"))
    pivot = cohort.pivot(index="cohort_month", columns="month_offset", values="cumulative_repurchase_rate")
    fig = go.Figure(go.Heatmap(z=pivot.values, x=[int(c) for c in pivot.columns], y=[d.strftime("%Y-%m") for d in pivot.index],
                               colorscale=[[i / 6, c] for i, c in enumerate(theme.SEQUENTIAL)], colorbar_title="%",
                               hovertemplate="coorte %{y}, mês +%{x}: %{z:.1f}% já recompraram<extra></extra>"))
    fig.update_layout(title="Coorte de recompra: % acumulado da coorte que já comprou de novo", xaxis_title="meses desde a 1ª compra",
                      yaxis_title=None, yaxis_autorange="reversed", yaxis_type="category", height=480)
    ui.plot(fig, key="olist_cohort")

    st.subheader("Top vendedores")
    top = data.read_parquet(path("kpi_top_sellers.parquet")).head(15)
    show = top.assign(seller=top["seller_id"].str[:8] + "…")[["revenue_rank", "seller", "state", "orders", "revenue_items", "avg_review_score", "pct_late"]]
    show.columns = ["#", "vendedor", "UF", "pedidos", "receita de itens (R$)", "nota média", "% atrasados"]
    h.show_table(show)

    st.subheader("Testes do dbt")
    results = pd.DataFrame(run["results"])
    tests = results[results["resource_type"] == "test"]
    s = run["summary"]
    ui.metric_row([
        ("Testes que passaram", str(s.get("test_pass", 0))),
        ("Avisos", str(s.get("test_warn", 0)), "Testes com severidade `warn` que encontraram linhas."),
        ("Falhas", str(s.get("test_fail", 0) + s.get("test_error", 0))),
        ("Modelos construídos", str(s.get("model_success", 0))),
    ])
    st.caption(f"dbt {run['dbt_version']} · `dbt build` em {run['elapsed_seconds']} s · execução real, dados sintéticos.")
    flt = st.radio("Mostrar", ["Todos", "Só avisos e falhas", "Só testes próprios"], horizontal=True, key="olist_testfilter")
    view = tests
    if flt == "Só avisos e falhas":
        view = tests[tests["status"] != "pass"]
    elif flt == "Só testes próprios":
        view = tests[tests["test_name"].isin(["non_negative", "column_not_before", "unique_combination"]) | (tests["test_kind"] == "singular")]
    table = pd.DataFrame({
        "status": view["status"].map(h.status_label), "teste": view["test_name"], "tipo": view["test_kind"],
        "modelo/fonte": view["model"], "coluna": view["column"], "linhas com problema": view["failures"], "severidade": view["severity"],
    })
    h.show_table(table, height=360)
    st.caption("Teste com aviso: o gerador sintético replica de propósito um defeito do Olist real (poucos pedidos 'delivered' sem data de entrega) para exercitar a severidade `warn`.")

with demo:
    st.subheader("Simulador de prazo prometido (SLA)")
    st.markdown(
        "Se a **data prometida** ao cliente fosse deslocada em *s* dias, qual % dos pedidos entregues seria considerado atrasado? "
        "Cálculo feito agora em DuckDB sobre `fct_orders` (pedidos entregues com data de entrega conhecida)."
    )
    fct = path("fct_orders.parquet")
    states = data.query("select distinct customer_state s from fct where is_delivered and delay_days is not null order by 1", {"fct": fct})["s"].tolist()
    c1, c2 = st.columns([2, 1])
    shift = c1.slider("Deslocamento da data prometida (dias)", -10, 10, 0, key="olist_shift",
                      help="Negativo = promessa mais curta (mais atrasos); positivo = promessa mais folgada.")
    chosen_states = c2.multiselect("UFs (vazio = todas)", states, key="olist_states")
    where = "is_delivered and delay_days is not null"
    params: list = []
    if chosen_states:
        where += f" and customer_state in ({', '.join('?' for _ in chosen_states)})"
        params = list(chosen_states)
    curve = data.query(
        f"select s as shift, 100.0 * avg(cast(delay_days > s as integer)) as pct_late, avg(estimated_days + s) as promised_days, count(*) as n "
        f"from fct, (select unnest(range(-10, 11)) as s) where {where} group by s order by s",
        {"fct": fct}, params)
    cur = curve[curve["shift"] == shift].iloc[0]
    ui.metric_row([
        ("% atrasados com a nova promessa", f"{cur['pct_late']:.1f}%".replace(".", ",")),
        ("Prazo médio prometido", f"{cur['promised_days']:.1f} dias".replace(".", ",")),
        ("Pedidos considerados", h.fmt_int(cur["n"])),
    ])
    fig = go.Figure(go.Scatter(x=curve["shift"], y=curve["pct_late"], mode="lines", line_color=theme.CATEGORICAL[1], name="% atrasados",
                               hovertemplate="deslocamento %{x} dias: %{y:.1f}% atrasados<extra></extra>"))
    fig.add_trace(go.Scatter(x=[shift], y=[cur["pct_late"]], mode="markers", marker=dict(size=11, color=theme.CATEGORICAL[1]), name="seleção", showlegend=False))
    fig.update_layout(title="% atrasados × deslocamento da promessa", xaxis_title="deslocamento (dias)", yaxis_title="% atrasados")
    ui.plot(fig, key="olist_whatif")

    st.divider()
    st.subheader("Explorador das tabelas gold")
    table_names = list(meta["rows"])
    sel = st.selectbox("Tabela", table_names, index=table_names.index("kpi_top_sellers"), key="olist_table")
    tpath = path(f"{sel}.parquet")
    columns = data.query("select * from t limit 0", {"t": tpath}).columns.tolist()
    f1, f2, f3 = st.columns([2, 2, 1])
    fcol = f1.selectbox("Filtrar coluna (texto contém)", ["(nenhum)"] + columns, key="olist_fcol")
    fval = f2.text_input("Valor", key="olist_fval")
    limit = f3.number_input("Linhas", 5, 500, 25, step=5, key="olist_limit")
    sql = "select * from t"
    params = []
    if fcol != "(nenhum)" and fval:
        sql += f' where cast("{fcol}" as varchar) ilike ?'
        params = [f"%{fval}%"]
    sql += f" limit {int(limit)}"
    st.code(sql.replace("?", f"'%{fval}%'") if params else sql, language="sql")
    h.show_table(data.query(sql, {"t": tpath}, params))
