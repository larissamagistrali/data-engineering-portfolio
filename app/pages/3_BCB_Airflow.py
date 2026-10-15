"""Página 3: ingestão diária de séries do BCB com Airflow (bronze -> silver -> gold, incremental)."""
from datetime import date

import _bootstrap  # noqa: F401
import _helpers as h
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from bcb_ingestion.indices import UNIDADES, nivel_comparavel, variacao_no_periodo

from shared import data, theme, ui

PROJECT = "bcb-airflow-ingestion"
# Ordem canônica fixa: a cor de cada série não muda quando o visitante filtra.
SERIES_ORDER = ["selic_meta", "dolar_venda", "ipca_mensal", "selic_over_mensal", "igpm_mensal"]
COLORS = theme.color_map(SERIES_ORDER)
TASKS_DOT = """digraph G {
  rankdir=LR; bgcolor="transparent"; nodesep=0.25; ranksep=0.5;
  node [shape=box, style="rounded,filled", fontname="Helvetica", fontsize=11, margin="0.12,0.06", fillcolor="%(grid)s", color="%(grid)s", fontcolor="%(ink)s"];
  edge [color="%(base)s", arrowsize=0.6];
  start [label="start", shape=circle, fillcolor="%(ink2)s", color="%(ink2)s", fontcolor="#ffffff", width=0.5];
  end [label="end", shape=circle, fillcolor="%(ink2)s", color="%(ink2)s", fontcolor="#ffffff", width=0.5];
  subgraph cluster_fetch { label="fetch (mapeado por série)"; fontname="Helvetica"; fontsize=11; fontcolor="%(ink2)s"; color="%(grid)s"; style="rounded";
    %(fetch_nodes)s }
  subgraph cluster_validate { label="validate (mapeado por série)"; fontname="Helvetica"; fontsize=11; fontcolor="%(ink2)s"; color="%(grid)s"; style="rounded";
    %(validate_nodes)s }
  write [label="write\\nsérie gold + status", fillcolor="%(ink2)s", color="%(ink2)s", fontcolor="#ffffff"];
  start -> %(first_fetch)s;
  %(edges)s
  write -> end;
}"""



def indices_base100(df: pd.DataFrame, nomes: list[str]) -> tuple[go.Figure | None, list[str]]:
    """Índice base 100 só para séries comparáveis; devolve a figura e as taxas que ficaram de fora."""
    fig, fora = go.Figure(), []
    for nome in SERIES_ORDER:
        sub = df[df["serie"] == nome].sort_values("data")
        if nome not in nomes or sub.empty:
            continue
        nivel = nivel_comparavel(sub["valor"], UNIDADES[nome])
        if nivel is None:
            fora.append(nome)
            continue
        rotulo = nome.replace("_mensal", " acumulado") if UNIDADES[nome] == "% a.m." else nome
        fig.add_trace(go.Scatter(x=sub["data"], y=nivel / nivel.iloc[0] * 100, mode="lines", name=rotulo,
                                 line=dict(color=COLORS[nome], width=2),
                                 hovertemplate="%{x|%d/%m/%Y}: %{y:,.1f}<extra>" + rotulo + "</extra>"))
    return (fig if fig.data else None), fora


def legenda_base100(fora: list[str]) -> None:
    st.caption("Variações mensais (IPCA, IGP-M) entram como índice acumulado, Π(1 + r/100); o câmbio entra direto.")
    if fora:
        st.caption(f"Fora do índice: {', '.join(fora)}. Base 100 de uma taxa mede a variação da própria taxa e não se "
                   "compara a um índice de preços.")


ui.setup_page("BCB · Airflow", "🏦")
ui.header(
    "Ingestão de séries do Banco Central com Airflow",
    "Carga diária e incremental de séries do SGS, validada e consolidada em camadas bronze, silver e gold.",
    tags=["Apache Airflow 3", "TaskFlow API", "dynamic task mapping", "pandas", "Parquet"],
    code_path="projects/bcb-airflow-ingestion",
)
h.pipeline_note()

problema, dados, abordagem, resultado, demo = ui.tabs()

GOLD = data.data_path(PROJECT, "series_gold.parquet")
STATUS = data.data_path(PROJECT, "series_status.json")
RUNS = data.data_path(PROJECT, "runs.json")
if h.missing_data([GOLD, STATUS, RUNS], "Execute o pipeline como descrito no README do projeto para gerar os artefatos."):
    st.stop()

gold = data.read_parquet(str(GOLD))
gold["data"] = pd.to_datetime(gold["data"])
status = data.read_json(str(STATUS))
runs = data.read_json(str(RUNS))
series_status = {s["serie"]: s for s in status["series"]}

with problema:
    st.markdown(
        """
**Pergunta:** como manter um conjunto de séries macroeconômicas (juros, câmbio e inflação) sempre atualizado,
sem baixar tudo a cada dia, sem gravar dado quebrado e sem depender de alguém rodar um script à mão?

**O que o pipeline faz:** uma DAG diária busca cada série na API pública do SGS, só o que falta desde a última data
gravada (carga incremental), valida o resultado e consolida tudo em uma tabela longa. Cada série é uma tarefa mapeada,
então uma falha em uma série não derruba as outras, e a tarefa pode ser reexecutada com retries.
"""
    )

with dados:
    ui.data_notice(
        "real",
        "API SGS do Banco Central (dados públicos, sem autenticação)",
        "Séries: Selic meta (432), dólar americano venda (1), IPCA mensal (433), Selic over mensal (4189) "
        "e IGP-M mensal (189). Dados do próprio BCB, reproduzidos com citação da fonte.",
    )
    st.markdown(
        """
- **Endpoint:** `api.bcb.gov.br/dados/serie/bcdata.sgs.{código}/dados`, em JSON, com janelas de até 5 anos por chamada
  (a API limita a 10 anos para séries diárias).
- **Identificação:** cabeçalho `User-Agent` descritivo; retry com backoff exponencial em 429, 5xx e erros de rede.
- **Camadas:** `bronze` = resposta JSON bruta por janela; `silver` = uma tabela Parquet por série, sem duplicatas;
  `gold` = tabela longa `data, serie, codigo, valor` e `series_status.json`.
"""
    )
    st.dataframe(
        pd.DataFrame(status["series"])[["codigo", "serie", "primeira_data", "ultima_data", "linhas"]]
        .rename(columns={"codigo": "código SGS", "serie": "série", "primeira_data": "primeira data",
                         "ultima_data": "última data", "linhas": "linhas"}),
        hide_index=True, width="stretch",
    )

with abordagem:
    st.markdown(
        """
- **Incremental:** a coleta começa na última data gravada na silver (inclusive, para capturar revisões do BCB).
  A escrita é idempotente: reexecutar com os mesmos dados não cria linhas duplicadas.
- **Validação antes da gold:** datas nulas, valores não numéricos ou nulos, datas duplicadas por série e datas futuras
  falham a tarefa `validate` da série afetada.
- **Airflow:** TaskFlow API, `fetch.expand(codigo=...)` (dynamic task mapping), `retries=2`, agendamento diário
  e `catchup=False`. A DAG só orquestra funções puras do pacote `bcb_ingestion`, que são testadas sem Airflow.
"""
    )
    st.subheader("DAG")
    fetch_nodes = "\n    ".join(f'f_{c} [label="fetch\\n{SERIES_ORDER[i]}"];' for i, c in enumerate(SERIES_ORDER))
    val_nodes = "\n    ".join(f'v_{c} [label="validate\\n{SERIES_ORDER[i]}"];' for i, c in enumerate(SERIES_ORDER))
    codes = [432, 1, 433, 4189, 189]
    edges = "\n  ".join(f"f_{c} -> v_{c};\n  v_{c} -> write;" for c in codes)
    dot = TASKS_DOT % {
        "grid": theme.GRID, "ink": theme.INK, "ink2": theme.INK_SECONDARY, "base": theme.BASELINE,
        "fetch_nodes": fetch_nodes.replace("f_", "f_"), "validate_nodes": val_nodes,
        "first_fetch": " ".join(f"f_{c}" for c in codes[:1]), "edges": f"start -> {{{' '.join(f'f_{c}' for c in codes)}}};\n  " + edges,
    }
    st.graphviz_chart(dot, width="stretch")
    st.caption("Gerado a partir da estrutura da DAG `bcb_ingestion` (fetch e validate são mapeados por série).")

with resultado:
    st.subheader("Séries atualizadas (últimos 24 meses, um painel por série)")
    recent = gold[gold["data"] >= gold["data"].max() - pd.DateOffset(months=24)].sort_values("data")
    fig = px.line(recent, x="data", y="valor", color="serie", facet_col="serie", facet_col_wrap=1, facet_row_spacing=0.08,
                  category_orders={"serie": SERIES_ORDER}, color_discrete_map=COLORS, height=170 * len(SERIES_ORDER))
    fig.update_yaxes(matches=None, title_text="", showticklabels=True)
    fig.update_xaxes(title_text="")
    fig.for_each_annotation(lambda a: a.update(text=f"{a.text.split('=', 1)[-1]} ({UNIDADES[a.text.split('=', 1)[-1]]})"))
    fig.update_layout(showlegend=False)
    ui.plot(fig, key="bcb_recent")

    st.subheader("Comparação em índice base 100 (histórico completo)")
    fig, fora = indices_base100(gold, SERIES_ORDER)
    fig.update_layout(title="Índice base 100 = primeira observação de cada série", yaxis_title=None, xaxis_title=None)
    ui.plot(fig, key="bcb_index")
    legenda_base100(fora)

    st.subheader("Última atualização por série")
    upd = pd.DataFrame([{
        "série": s["serie"], "código SGS": s["codigo"], "última data": s["ultima_data"],
        "linhas": s["linhas"], "linhas novas na última execução": s["linhas_novas_na_execucao"],
    } for s in status["series"]])
    h.show_table(upd)
    st.caption(f"Status gerado em {status['gerado_em_utc']} (UTC). {status['fonte']}.")

    st.subheader("Execuções do Airflow")
    rows = []
    for e in runs["execucoes"]:
        rows.append({
            "data lógica": str(e["data_logica"])[:10],
            "estado": h.status_label(e["estado"]),
            "tarefas com sucesso": f"{e['tarefas_sucesso']}/{e['tarefas_total']}",
            "duração (s)": e["duracao_s"],
            "início (UTC)": str(e["inicio_utc"]).replace("T", " ")[:19],
            "modo": e["tipo"],
        })
    h.show_table(pd.DataFrame(rows))
    st.caption(runs["modo"] + ". Execuções locais registradas no metastore; não são execuções de um scheduler em produção.")

with demo:
    st.markdown("Escolha séries e período. O índice base 100 é recalculado a partir da primeira observação do período "
                "(só para câmbio e variações mensais; taxas aparecem na tabela, em p.p.).")
    min_d, max_d = gold["data"].min().date(), gold["data"].max().date()
    escolhidas = st.multiselect("Séries", SERIES_ORDER, default=["dolar_venda", "ipca_mensal", "igpm_mensal"])
    periodo = st.date_input("Período", value=(date(2020, 1, 1), max_d), min_value=min_d, max_value=max_d)
    if isinstance(periodo, tuple) and len(periodo) == 2:
        ini, fim = periodo
    else:
        ini, fim = min_d, max_d
    if not escolhidas:
        st.info("Selecione ao menos uma série.")
    else:
        sel = gold[(gold["serie"].isin(escolhidas)) & (gold["data"] >= pd.Timestamp(ini)) & (gold["data"] <= pd.Timestamp(fim))]
        fig, fora = indices_base100(sel, escolhidas)
        if fig is None:
            st.info("As séries escolhidas são taxas e não entram no índice base 100; a variação delas, em p.p., está na tabela.")
        else:
            fig.update_layout(title="Índice base 100 no período escolhido", yaxis_title=None, xaxis_title=None)
            ui.plot(fig, key="bcb_demo")
        legenda_base100(fora)
        resumo = []
        for n in escolhidas:
            v = sel[sel["serie"] == n].sort_values("data")["valor"]
            if len(v) < 2:
                continue
            var, unid = variacao_no_periodo(v, UNIDADES[n])
            resumo.append({"série": n, "natureza": UNIDADES[n], "valor inicial": round(float(v.iloc[0]), 4),
                           "valor final": round(float(v.iloc[-1]), 4), "variação no período": f"{var:+.2f} {unid}"})
        h.show_table(pd.DataFrame(resumo))
