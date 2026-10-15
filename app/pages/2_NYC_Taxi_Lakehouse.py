"""Página 2: lakehouse medalhão com PySpark (bronze -> silver -> gold) sobre as corridas de táxi amarelo de NYC."""
import _bootstrap  # noqa: F401
import _helpers as h
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from shared import data, theme, ui

PROJECT = "nyc-taxi-lakehouse"
DOW_ORDER = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]


RULE_LABELS = {
    "null_timestamp": "timestamp nulo",
    "dropoff_before_pickup": "dropoff antes do pickup",
    "outside_source_month": "fora do mês do arquivo",
    "invalid_distance": "distância inválida",
    "invalid_fare": "tarifa inválida",
    "invalid_total": "total inválido",
    "invalid_passengers": "passageiros inválidos",
    "duplicate_row": "linha duplicada",
}


def path(name: str) -> str:
    return str(data.data_path(PROJECT, name))


def fmt_usd(value: float) -> str:
    """Valores da TLC estão em dólares americanos (US$), no mesmo formato numérico do helper."""
    return h.fmt_brl(value).replace("R$", "US$")


def num(value) -> float:
    """SUM sem linhas devolve NaN; trata como zero."""
    return 0.0 if pd.isna(value) else float(value)


ui.setup_page("NYC Taxi · Lakehouse", "🚕")
ui.header(
    "Lakehouse de corridas de táxi em NYC com PySpark",
    "Dados brutos da TLC aos indicadores por hora, dia e zona, com regras de qualidade medidas em cada etapa.",
    tags=["PySpark 4.2", "Parquet", "arquitetura medalhão", "Java 17"],
    code_path="projects/nyc-taxi-lakehouse",
)
h.pipeline_note()

problema, dados, abordagem, resultado, demo = ui.tabs()

required = [data.data_path(PROJECT, f) for f in (
    "gold_overview.parquet", "gold_daily.parquet", "gold_heatmap_hour_dow.parquet", "gold_zone.parquet",
    "gold_zone_hour.parquet", "gold_od_top.parquet", "quality_report.json")]
if h.missing_data(required, "Rode o pipeline (`make data-nyc-taxi-lakehouse`) para gerar a camada gold."):
    st.stop()

overview = data.read_parquet(path("gold_overview.parquet")).iloc[0]
daily = data.read_parquet(path("gold_daily.parquet")).sort_values("trip_date")  # o gold do Spark não garante ordem
heat = data.read_parquet(path("gold_heatmap_hour_dow.parquet"))
zones = data.read_parquet(path("gold_zone.parquet"))
od_top = data.read_parquet(path("gold_od_top.parquet"))
report = data.read_json(path("quality_report.json"))

with problema:
    st.markdown(
        """
**Pergunta de negócio:** quando e onde a demanda de táxis amarelos de Nova York se concentra, e quanto
de receita e de gorjeta cada hora, dia e zona gera? Para responder com confiança, o dado bruto da TLC precisa
passar por limpeza, validação e agregação de forma auditável.

**Por que um lakehouse em camadas:** cada etapa tem uma responsabilidade clara. A bronze guarda o dado como chegou,
a silver aplica tipagem, regras de qualidade e deduplicação, e a gold entrega agregados pequenos para análise.
Quem lê um número consegue voltar até a linha de origem e ver quantas linhas foram descartadas e por qual regra.
"""
    )

with dados:
    ui.data_notice(
        "real",
        "NYC TLC Yellow Taxi Trip Records (2024-01 e 2024-02) e Taxi Zone Lookup",
        "Dados públicos da Taxi & Limousine Commission de NYC, sem alteração de conteúdo. "
        "Esta página lê só a camada gold e o relatório de qualidade gerados pelo pipeline.",
    )
    st.markdown(
        f"""
- **Volume bruto:** {h.fmt_int(report['totals']['bronze_rows'])} viagens em dois arquivos mensais.
- **Campos principais:** pickup e dropoff, distância, tarifa, gorjeta, pagamento, zonas de embarque e desembarque.
- **Zonas:** tabela de lookup da TLC com 265 zonas, bairro e tipo de serviço.
- **Licença:** dados públicos da TLC, usados com atribuição à fonte.
"""
    )

with abordagem:
    st.markdown("**Fluxo de dados** (cada seta é uma transformação executada no PySpark local)")
    st.graphviz_chart(
        """
digraph G {
    rankdir=LR; bgcolor="transparent";
    node [shape=box, style="rounded,filled", fontname="Helvetica", fontsize=11, color="#c3c2b7", fillcolor="#f0efec"];
    edge [color="#898781", fontname="Helvetica", fontsize=10];
    raw [label="Arquivos brutos\\nParquet TLC + CSV de zonas"];
    bronze [label="Bronze\\ningestão fiel + source_file,\\nsource_month, ingested_at"];
    silver [label="Silver\\ntipagem, regras de qualidade,\\ndeduplicação, zonas"];
    gold [label="Gold\\nagregados por hora, dia,\\nzona e par origem-destino"];
    app [label="App\\nlê só o gold e o relatório"];
    raw -> bronze [label="ingestão"];
    bronze -> silver [label="validação"];
    silver -> gold [label="agregação"];
    gold -> app [label="Parquet"];
}
"""
    )
    st.markdown(
        """
**Regras de qualidade (silver).** Cada linha rejeitada conta só na primeira regra que viola:
pickup ou dropoff nulo, dropoff antes do pickup, pickup fora do mês do arquivo, distância inválida,
tarifa inválida, total negativo e número de passageiros inválido. Depois, linhas idênticas são removidas.
A soma das rejeições mais as linhas aceitas bate exatamente com o total da bronze, e o pipeline
verifica essa igualdade antes de gravar.

**Formato e particionamento.** Parquet com compressão zstd, bronze particionada por mês de origem e silver
particionada por mês de embarque. Não usamos Delta Lake nesta versão.
"""
    )

with resultado:
    totals = report["totals"]
    ui.metric_row([
        ("Viagens válidas", h.fmt_int(overview["trips"]), "Linhas da silver, após regras e deduplicação"),
        ("Receita total", fmt_usd(overview["revenue_total"]), "Soma de total_amount"),
        ("Gorjeta sobre tarifa", f"{overview['tip_pct']:.1f}%", "Soma das gorjetas / soma das tarifas"),
        ("Duração média", f"{overview['avg_duration_min']:.1f} min"),
        ("Distância média", f"{overview['avg_distance_mi']:.2f} mi"),
    ])

    st.subheader("Funil bronze → silver")
    ui.metric_row([
        ("Bronze (bruto)", h.fmt_int(totals["bronze_rows"]), "Linhas lidas dos Parquet da TLC"),
        ("Rejeitadas", h.fmt_int(totals["rejected_rows"]), "Cada linha conta só na primeira regra que viola"),
        ("Silver (válido)", h.fmt_int(totals["silver_rows"]), f"{totals['silver_share_pct']:.2f}% da bronze"),
    ])
    # Barras só das rejeições: na escala de ~6 milhões da bronze, regras com dezenas de linhas somem.
    rule_rows = sorted((r for r in report["rules"] if r["rows_rejected"] > 0), key=lambda r: r["rows_rejected"])
    rejected = go.Figure(
        go.Bar(
            x=[r["rows_rejected"] for r in rule_rows],
            y=[RULE_LABELS.get(r["rule"], r["description"]) for r in rule_rows],
            orientation="h",
            marker_color=theme.INK_MUTED,
            text=[h.fmt_int(r["rows_rejected"]) for r in rule_rows],
            textposition="outside",
            cliponaxis=False,
            customdata=[r["description"] for r in rule_rows],
            hovertemplate="%{customdata}<extra></extra>",
        )
    )
    rejected.update_layout(
        title="Linhas rejeitadas por regra",
        xaxis_title="linhas rejeitadas",
        xaxis_range=[0, max(r["rows_rejected"] for r in rule_rows) * 1.3],
        height=70 + 42 * len(rule_rows),
    )
    ui.plot(rejected, key="funnel")
    st.caption(
        "Passe o mouse numa barra para ver a regra completa. Zonas sem correspondência no lookup: "
        f"{h.fmt_int(totals['unmapped_pickup_zone_rows'])} linhas."
    )

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Mapa de calor: viagens por hora e dia")
        pivot = heat.pivot_table(index="dow_name", columns="pickup_hour", values="trips", aggfunc="sum")
        pivot = pivot.reindex(DOW_ORDER)
        scale = [[i / (len(theme.SEQUENTIAL) - 1), c] for i, c in enumerate(theme.SEQUENTIAL)]
        heatmap = go.Figure(
            go.Heatmap(
                z=pivot.values,
                x=[f"{hr:02d}h" for hr in pivot.columns],
                y=list(pivot.index),
                colorscale=scale,
                colorbar=dict(title="viagens"),
                hovertemplate="%{y} %{x}: %{z:,.0f} viagens<extra></extra>",
            )
        )
        heatmap.update_layout(height=340, yaxis=dict(autorange="reversed"))
        ui.plot(heatmap, key="heatmap")
    with col2:
        st.subheader("Zonas de embarque com mais viagens")
        top = zones.sort_values("trips", ascending=False).head(10).iloc[::-1]
        bars = go.Figure(
            go.Bar(
                x=top["trips"],
                y=top["pu_zone"],
                orientation="h",
                marker_color=theme.CATEGORICAL[0],
                customdata=top["pu_borough"],
                hovertemplate="%{y} (%{customdata}): %{x:,.0f} viagens<extra></extra>",
            )
        )
        bars.update_layout(height=340, xaxis_title="viagens", showlegend=False)
        ui.plot(bars, key="top_zones")

    st.subheader("Evolução diária")
    line = go.Figure(
        go.Scatter(
            x=daily["trip_date"],
            y=daily["trips"],
            mode="lines",
            line=dict(color=theme.CATEGORICAL[0], width=2),
            name="viagens",
            hovertemplate="%{x|%d/%m}: %{y:,.0f} viagens<extra></extra>",
        )
    )
    line.update_layout(yaxis_title="viagens por dia", height=300, showlegend=False)
    ui.plot(line, key="daily")

    st.subheader("Pares origem-destino mais frequentes")
    show = od_top.rename(columns={
        "pu_zone": "Origem", "do_zone": "Destino", "trips": "Viagens",
        "avg_distance_mi": "Distância média (mi)", "avg_duration_min": "Duração média (min)",
    }).round({"Distância média (mi)": 2, "Duração média (min)": 1})
    h.show_table(show, height=360)

with demo:
    st.markdown("Escolha uma zona de embarque e uma faixa de horas. A resposta vem de uma consulta SQL sobre a tabela gold.")
    zone_names = sorted(zones["pu_zone"].unique())
    default = zone_names.index("Midtown Center") if "Midtown Center" in zone_names else 0
    zone = st.selectbox("Zona de embarque", zone_names, index=default, key="demo_zone")
    lo, hi = st.slider("Faixa de horas do embarque", 0, 23, (7, 10), key="demo_hours")

    tables = {"zh": path("gold_zone_hour.parquet")}
    answer = data.query(
        """
        select sum(trips) as trips, sum(revenue_total) as revenue,
               sum(tip_total) / sum(fare_total) * 100 as tip_pct
        from zh where pu_zone = ? and pickup_hour between ? and ?
        """,
        tables,
        [zone, int(lo), int(hi)],
    ).iloc[0]
    hourly = data.query(
        "select pickup_hour, sum(trips) as trips from zh where pu_zone = ? group by 1 order by 1",
        tables,
        [zone],
    )

    trips_in_range = int(num(answer["trips"]))
    ui.metric_row([
        ("Viagens na faixa", h.fmt_int(trips_in_range)),
        ("Receita na faixa", fmt_usd(num(answer["revenue"]))),
        ("Gorjeta sobre tarifa", f"{num(answer['tip_pct']):.1f}%"),
    ])
    st.caption(
        f"{zone}, entre {lo:02d}h e {hi:02d}h: {h.fmt_int(trips_in_range)} viagens "
        f"({trips_in_range / max(float(overview['trips']), 1) * 100:.2f}% do total de viagens válidas)."
    )
    selected = hourly["pickup_hour"].between(lo, hi)
    fig = go.Figure(
        go.Bar(
            x=hourly["pickup_hour"],
            y=hourly["trips"],
            marker_color=[theme.CATEGORICAL[0] if s else theme.BASELINE for s in selected],
            hovertemplate="%{x}h: %{y:,.0f} viagens<extra></extra>",
        )
    )
    fig.update_layout(xaxis=dict(title="hora do embarque", dtick=1), yaxis_title="viagens", height=320, showlegend=False)
    ui.plot(fig, key="zone_hours")
    st.caption("Em destaque: faixa escolhida. Gorjeta registrada só em pagamentos com cartão (ver Limitações no README).")
