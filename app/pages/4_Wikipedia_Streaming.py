"""Página 4: streaming de edições da Wikipedia (SSE -> Kafka -> agregação em janelas de 1 min)."""
import _bootstrap  # noqa: F401
from decimal import ROUND_HALF_UP, Decimal

import _helpers as h
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from shared import data, theme, ui

PROJECT = "wikipedia-streaming"
AGG = "agregado_por_minuto.parquet"
EDITOR_TYPES = ["humano", "bot"]
EVENT_TYPES = ["edit", "new", "log", "categorize"]
COVERAGE = ["completa", "parcial (borda da captura)"]
EDITOR_COLORS = theme.color_map(EDITOR_TYPES)
COVERAGE_COLORS = {COVERAGE[0]: theme.SEQUENTIAL[4], COVERAGE[1]: theme.SEQUENTIAL[1]}
BAR_COLOR = theme.SEQUENTIAL[4]

ARCHITECTURE_DOT = """
digraph G {
  rankdir=LR; bgcolor="transparent";
  node [shape=box, style="rounded", fontname="system-ui", fontsize=11, color="#c3c2b7", fontcolor="#0b0b0b"];
  edge [color="#898781", fontname="system-ui", fontsize=10, fontcolor="#52514e"];
  sse [label="Wikimedia EventStreams\\nrecentchange (SSE)\\n1 conexão"];
  prod [label="producer.py\\nparse SSE, backoff,\\nLast-Event-ID"];
  kafka [label="Kafka 4.3.1 (KRaft)\\ntópico wiki-recentchange\\n3 partições, chave = wiki"];
  agg [label="aggregator.py\\njanelas tumbling 1 min\\nwatermark 2 min"];
  pq [label="Parquet + metrics.json\\nprojects/.../data/"];
  app [label="App Streamlit\\n(esta página)"];
  late [label="tardios (> 2 min)\\ncontados e descartados", shape=note, fontcolor="#52514e"];
  sse -> prod -> kafka -> agg -> pq -> app;
  agg -> late [style=dashed];
}
"""


def path(name: str) -> str:
    return str(data.data_path(PROJECT, name))


def fmt_s(value) -> str:
    """Segundos com 1 casa, arredondamento half-up sobre o valor gravado (176.45 -> '176,5 s')."""
    if value is None:
        return "n/d"
    rounded = Decimal(str(value)).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
    rounded = abs(rounded) if rounded == 0 else rounded  # evita "-0,0 s"
    return f"{rounded:,.1f} s".replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_utc(iso: str, fmt: str = "%d/%m/%Y %H:%M:%S") -> str:
    return pd.Timestamp(iso).tz_convert("UTC").strftime(fmt)


def show(fig: go.Figure, key: str) -> None:
    """ui.plot com chave estável (margens, título e legenda já são tratados pelo tema)."""
    ui.plot(fig, key=key)


def fmt_pct(value: float) -> str:
    return f"{value:.0%}".replace(".", ",")


ui.setup_page("Wikipedia · Streaming", "📡")
ui.header(
    "Wikipedia em tempo real: streaming com Kafka",
    "Edições da Wikipedia chegam por SSE, passam pelo Kafka e viram agregados por minuto, com latência medida.",
    tags=["Kafka 4.3.1 (KRaft)", "confluent-kafka", "SSE", "janelas + watermark", "Parquet"],
    code_path="projects/wikipedia-streaming",
)
h.pipeline_note()

problema, dados, abordagem, resultado, demo = ui.tabs()

required = [data.data_path(PROJECT, f) for f in (AGG, "metrics.json")]
if h.missing_data(required, "Rode o pipeline (ver README do projeto) para gerar os agregados."):
    st.stop()

agg = data.read_parquet(path(AGG)).copy()
agg["cobertura"] = agg["janela_parcial"].map({False: COVERAGE[0], True: COVERAGE[1]})
metrics = data.read_json(path("metrics.json"))
cap = metrics["captura"]
cnt = metrics["contagens"]
lat_all = metrics["latencia_segundos"]
lat = lat_all["ponta_a_ponta_gravacao"]
lat_forced = lat_all["ponta_a_ponta_gravacao_fim_do_run"]
lat_cons = lat_all["ate_consumo"]
disorder = lat_all["desordem_observada"]
hist = lat_all["histograma_gravacao"]
offset = metrics.get("relogio_local_offset_segundos")
consume_help = "meta.dt → leitura pelo aggregator, medido com o relógio local, sem correção."
if offset is not None:
    consume_help += (f" O relógio local estava {fmt_s(abs(offset))} {'atrás' if offset > 0 else 'à frente'} do NTP; "
                     f"corrigido, o p50 fica em {fmt_s(lat_cons['p50'] + offset)}.")
producer = metrics.get("producer", {})

per_minute = agg.groupby("janela_inicio", as_index=False).agg(
    eventos=("eventos", "sum"), fechada=("fechada", "all"), cobertura=("cobertura", "first")
)
per_minute_editor = (
    agg.pivot_table(index="janela_inicio", columns="tipo_editor", values="eventos", aggfunc="sum", fill_value=0)
    .reindex(columns=EDITOR_TYPES, fill_value=0)
    .reset_index()
)
per_wiki = agg.groupby("wiki", as_index=False)["eventos"].sum().sort_values("eventos", ascending=False)
WIKI_ORDER = per_wiki["wiki"].tolist()  # ordem canônica: total do período capturado (a cor segue a wiki)
WIKI_COLORS = theme.color_map(WIKI_ORDER)
total_by_editor = agg.groupby("tipo_editor")["eventos"].sum().reindex(EDITOR_TYPES, fill_value=0)
total = int(total_by_editor.sum())

with problema:
    st.markdown(
        """
**Pergunta de negócio:** como acompanhar, em tempo quase real, o volume e o perfil das edições da Wikipedia
(quais wikis, quanto é de bot, quais tipos de evento), sem consultar a API de minuto em minuto?

**Por que streaming:** a Wikimedia publica cada mudança como evento (EventStreams, via SSE). Um producer lê esse
fluxo com **uma única conexão** e publica no Kafka; um agregador consome o tópico e fecha janelas de 1 minuto.
A latência de ponta a ponta é **medida** em cada evento, não estimada.

**O que o projeto entrega**
- Producer com reconexão (backoff exponencial e `Last-Event-ID`) e descarte de reenvios.
- Agregador com janelas tumbling de 1 minuto e watermark de 2 minutos: eventos mais atrasados são contados como
  tardios e descartados do agregado, com a contagem reportada.
- Funções puras de janela e watermark, testadas com eventos sintéticos (sem rede e sem Kafka).
"""
    )

with dados:
    ui.data_notice(
        "real",
        "Wikimedia EventStreams (recentchange)",
        f"Período capturado: **{fmt_utc(cap['inicio_utc'])}** a "
        f"**{fmt_utc(cap['fim_utc'], '%H:%M:%S' if cap['inicio_utc'][:10] == cap['fim_utc'][:10] else '%d/%m/%Y %H:%M:%S')}** (UTC), "
        f"{fmt_s(cap['duracao_segundos'])} de consumo, com Kafka local. Uma única captura: é um retrato de "
        "cerca de 5 minutos, não uma série histórica.",
    )
    st.markdown(
        f"""
- **Fonte:** [Wikimedia EventStreams](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams_HTTP_Service),
  stream `recentchange` (feed de Recent Changes do MediaWiki), endpoint `https://stream.wikimedia.org/v2/stream/recentchange`.
- **Termos e licença:** a página do serviço não declara uma licença própria para os eventos e remete aos
  [Termos de Uso da Wikimedia Foundation](https://foundation.wikimedia.org/wiki/Policy:Terms_of_Use). O serviço
  público é voltado a ferramentas de pequena escala. Este projeto guarda só contagens agregadas.
- **Boas práticas:** User-Agent descritivo, conforme a [etiqueta da API](https://www.mediawiki.org/wiki/API:Etiquette),
  com uma conexão por vez, sem paralelismo, e reconexão com backoff e `Last-Event-ID`.
- **Campos usados:** `meta.dt` (timestamp do evento), `wiki`, `bot`, `type`. Títulos, comentários e usuários
  **não** são armazenados.
- **Agregado versionado:** `{AGG}` ({h.fmt_int(len(agg))} linhas), com uma linha por janela, wiki, tipo de
  editor e tipo de evento.
"""
    )
    counts = [
        ("Recebidos do SSE (producer)", producer.get("received")),
        ("Publicados no Kafka (confirmados pelo broker)", producer.get("published")),
        ("Reenvios descartados (deduplicação por meta.id)", producer.get("duplicates")),
        ("Reconexões ao SSE", producer.get("reconnects")),
        ("Consumidos do tópico (aggregator)", cnt["consumidos_do_topico"]),
        ("Agregados", cnt["agregados"]),
        ("Tardios descartados (watermark de 2 min)", cnt["tardios_descartados"]),
        ("Ignorados (tipo fora de edit/new/log/categorize)", cnt["ignorados_por_tipo"]),
        ("Inválidos", cnt["invalidos"]),
    ]
    h.show_table(pd.DataFrame(
        [(k, "n/d" if v is None else h.fmt_int(v)) for k, v in counts], columns=["Contagem", "Valor"]
    ))

with abordagem:
    st.markdown(
        """
**Fluxo:** SSE da Wikimedia → `producer.py` → Kafka (tópico `wiki-recentchange`, **chave = wiki**, ex.: `enwiki`)
→ `aggregator.py` → Parquet → este app.

**Decisões**
- **Chave por wiki:** eventos de uma mesma wiki caem na mesma partição e mantêm a ordem entre si.
- **Janela tumbling de 1 minuto** pelo timestamp do evento (`meta.dt`), não pelo horário de chegada.
- **Watermark = maior timestamp visto − 2 min.** Evento abaixo do watermark é tardio: é contado e descartado.
  A janela fecha (e o Parquet é regravado) quando o fim dela fica abaixo do watermark.
- **Fim do run:** sem eventos novos o watermark para. As janelas ainda abertas são gravadas com
  `fechada = false` e entram numa medição de latência separada.
- **Bordas:** a primeira e a última janela não são cobertas por inteiro pela captura (`janela_parcial = true`).
- **Latência de ponta a ponta:** para cada evento, instante em que sua janela foi gravada no Parquet menos `meta.dt`.
- **Desordem observada:** para cada evento, o quanto ele chegou atrás do maior timestamp já visto. É o dado
  que diz se a tolerância de 2 min está bem calibrada.
"""
    )

with resultado:
    ui.metric_row([
        ("Eventos agregados", h.fmt_int(cnt["agregados"])),
        ("Wikis distintas", h.fmt_int(agg["wiki"].nunique())),
        ("Latência p50 (ponta a ponta)", fmt_s(lat["p50"]), "Janelas fechadas pelo watermark: meta.dt → gravação no Parquet"),
        ("Latência p95 (ponta a ponta)", fmt_s(lat["p95"]), "Janelas fechadas pelo watermark"),
    ])
    ui.metric_row([
        ("Tardios descartados", h.fmt_int(cnt["tardios_descartados"])),
        ("Desordem máxima observada", fmt_s(disorder["max"]), "Maior atraso de um evento em relação ao maior timestamp já visto"),
        ("Até o consumo, p50 (bruto)", fmt_s(lat_cons["p50"]), consume_help),
        ("Participação de bots", fmt_pct(total_by_editor["bot"] / max(1, total))),
    ])

    st.subheader("Arquitetura executada")
    st.graphviz_chart(ARCHITECTURE_DOT, width="stretch")

    st.subheader("Volume")
    window_ms = metrics["janela_segundos"] * 1000
    fig = go.Figure()
    for cov in COVERAGE:
        part = per_minute[per_minute["cobertura"] == cov]
        fig.add_trace(go.Bar(
            x=part["janela_inicio"] + pd.Timedelta(milliseconds=window_ms / 2),  # barra ocupa o minuto [início, fim)
            y=part["eventos"], width=window_ms * 0.85, name=cov, marker_color=COVERAGE_COLORS[cov],
            customdata=part["janela_inicio"].dt.strftime("%H:%M"),
            hovertemplate="%{customdata} UTC: %{y} eventos<extra>" + cov + "</extra>",
        ))
    x_range = [per_minute["janela_inicio"].min(), per_minute["janela_inicio"].max() + pd.Timedelta(milliseconds=window_ms)]
    fig.update_layout(title="Edições por minuto (UTC)", yaxis_title="eventos", xaxis_title=None, barmode="overlay",
                      xaxis=dict(tickformat="%H:%M", dtick=window_ms, range=x_range))
    show(fig, key="por_minuto")
    forced = per_minute.loc[~per_minute["fechada"], "janela_inicio"]
    if not forced.empty:
        st.caption(
            f"{len(forced)} janela(s) a partir de {forced.min():%H:%M} UTC foram encerradas no fim do run "
            "(o watermark só avança com eventos novos). As contagens delas estão completas até o fim da captura."
        )

    c1, c2 = st.columns(2)
    with c1:
        top = per_wiki.head(10).sort_values("eventos")
        fig = go.Figure(go.Bar(x=top["eventos"], y=top["wiki"], orientation="h", marker_color=BAR_COLOR))
        fig.update_layout(title="Top 10 wikis no período", xaxis_title="eventos", yaxis_title=None, showlegend=False)
        show(fig, key="top_wikis")
    with c2:
        by_type = agg.groupby("tipo_evento")["eventos"].sum().reindex(EVENT_TYPES, fill_value=0).sort_values()
        fig = go.Figure(go.Bar(x=by_type.values, y=by_type.index, orientation="h", marker_color=BAR_COLOR))
        fig.update_layout(title="Eventos por tipo", xaxis_title="eventos", yaxis_title=None, showlegend=False)
        show(fig, key="tipos")

    st.subheader("Bots vs humanos")
    shares = {e: total_by_editor[e] / max(1, total) for e in EDITOR_TYPES}
    fig = go.Figure()
    for editor in EDITOR_TYPES:
        fig.add_trace(go.Bar(
            x=[shares[editor]], y=["período"], orientation="h", name=editor, marker_color=EDITOR_COLORS[editor],
            hovertemplate=f"{editor}: %{{x:.1%}} ({h.fmt_int(total_by_editor[editor])} eventos)<extra></extra>",
        ))
    fig.update_layout(
        title="Proporção no período: " + " · ".join(f"{e} {fmt_pct(shares[e])}" for e in EDITOR_TYPES),
        barmode="stack", height=230, xaxis=dict(tickformat=".0%", range=[0, 1.001], dtick=0.25),
        yaxis=dict(showticklabels=False), legend=dict(traceorder="normal"),
    )
    show(fig, key="bots_proporcao")
    fig = go.Figure()
    for editor in EDITOR_TYPES:
        fig.add_trace(go.Scatter(x=per_minute_editor["janela_inicio"], y=per_minute_editor[editor],
                                 mode="lines+markers", name=editor, line=dict(color=EDITOR_COLORS[editor])))
    fig.update_layout(title="Eventos por minuto, por tipo de editor (bordas da captura são parciais)",
                      yaxis_title="eventos", xaxis_title=None, xaxis=dict(tickformat="%H:%M", dtick=60_000))
    show(fig, key="bots_serie")

    st.subheader("Latência de ponta a ponta (medida)")
    window_s, lateness_s = metrics["janela_segundos"], metrics["atraso_permitido_segundos"]
    step = 15  # largura das faixas gravadas em metrics.json
    fig = go.Figure(go.Bar(
        x=[i * step + step / 2 for i in range(len(hist["eventos"]))], y=hist["eventos"], width=step * 0.9,
        customdata=hist["faixas_s"], hovertemplate="%{customdata} s: %{y} eventos<extra></extra>",
        marker_color=BAR_COLOR,
    ))
    refs = (
        (lateness_s, f"tolerância ({lateness_s} s)", "top left"),
        (window_s + lateness_s, f"janela + tolerância ({window_s + lateness_s} s)", "top right"),
    )
    for x, label, pos in refs:  # rótulos nas regiões vazias, fora das barras
        fig.add_vline(x=x, line_dash="dot", line_color=theme.INK_MUTED, line_width=1,
                      annotation_text=label, annotation_position=pos,
                      annotation_font=dict(color=theme.INK_SECONDARY, size=11))
    fig.update_layout(title="Do evento na Wikimedia à gravação no agregado (janelas fechadas pelo watermark)",
                      xaxis=dict(title="latência (s)", tickmode="linear", tick0=0, dtick=30),
                      yaxis_title="eventos", showlegend=False)
    show(fig, key="latencia")
    regimes = [
        ("Ponta a ponta: janelas fechadas pelo watermark", lat),
        ("Ponta a ponta: janelas encerradas no fim do run", lat_forced),
        ("Até o consumo pelo aggregator", lat_cons),
        ("Desordem observada (atraso vs. maior timestamp)", disorder),
    ]
    h.show_table(pd.DataFrame(
        [(name, h.fmt_int(s["n"]), fmt_s(s["p50"]), fmt_s(s["p95"]), fmt_s(s["max"])) for name, s in regimes],
        columns=["Medida", "n", "p50", "p95", "máx"],
    ))
    n_late = cnt["tardios_descartados"]
    late_text = ("nenhum evento passou da tolerância" if n_late == 0
                 else f"{h.fmt_int(n_late)} evento(s) passaram da tolerância e foram descartados")
    st.markdown(
        f"""
**Leitura.** Quase toda a latência vem da **tolerância de atraso escolhida**, não do Kafka. Uma janela de
{window_s} s só fecha quando o watermark (maior timestamp − {lateness_s} s) passa do fim dela, ou seja, cerca de
{window_s + lateness_s} s depois do início da janela. Um evento espera, portanto, entre {lateness_s} e
{window_s + lateness_s} s. A desordem real observada foi de no máximo **{fmt_s(disorder['max'])}** (p95
{fmt_s(disorder['p95'])}), e {late_text}. Uma tolerância menor deixaria o agregado mais
fresco, ao custo de descartar mais eventos em dias de maior desordem. Esse trade-off **não** foi medido aqui
com outras tolerâncias.
"""
    )
    if offset is not None:
        st.caption(
            f"Relógio: antes do run, `sntp` mediu o relógio local {fmt_s(abs(offset))} "
            f"{'atrás' if offset > 0 else 'à frente'} do NTP. Por isso a latência até o consumo aparece "
            f"perto de zero ou negativa. Corrigida por esse offset (supondo o relógio da Wikimedia sincronizado), "
            f"fica em p50 {fmt_s(lat_cons['p50'] + offset)} e p95 {fmt_s(lat_cons['p95'] + offset)}."
        )

with demo:
    st.markdown(
        "Escolha wikis, tipo de editor, tipo de evento e intervalo. Totais e série são recalculados "
        "sobre o Parquet do agregado."
    )
    minutes = sorted(agg["janela_inicio"].unique())
    c1, c2 = st.columns(2)
    wikis_sel = c1.multiselect("Wikis", WIKI_ORDER, default=WIKI_ORDER[:5])
    editor_sel = c2.radio("Tipo de editor", ["todos", "humano", "bot"], horizontal=True)
    c3, c4 = st.columns(2)
    events_sel = c3.multiselect("Tipo de evento", EVENT_TYPES, default=EVENT_TYPES)
    if len(minutes) > 1:
        lo, hi = c4.select_slider("Intervalo (minuto UTC)", options=minutes, value=(minutes[0], minutes[-1]),
                                  format_func=lambda t: pd.Timestamp(t).strftime("%H:%M"))
    else:
        lo = hi = minutes[0]
        c4.caption(f"Único minuto capturado: {pd.Timestamp(lo):%H:%M} UTC")
    only_complete = st.checkbox("Só janelas completas (exclui as bordas parciais da captura)", value=False)

    mask = (
        agg["wiki"].isin(wikis_sel)
        & agg["tipo_evento"].isin(events_sel)
        & (agg["janela_inicio"] >= lo)
        & (agg["janela_inicio"] <= hi)
    )
    if editor_sel != "todos":
        mask &= agg["tipo_editor"] == editor_sel
    if only_complete:
        mask &= ~agg["janela_parcial"]
    filtered = agg[mask]

    if filtered.empty:
        st.info("Nenhum evento com esses filtros.")
    else:
        partial_minutes = set(agg.loc[agg["janela_parcial"], "janela_inicio"])
        axis_minutes = [m for m in minutes if lo <= m <= hi and not (only_complete and m in partial_minutes)]
        n_minutes = max(1, len(axis_minutes))
        ui.metric_row([
            ("Eventos no filtro", h.fmt_int(filtered["eventos"].sum())),
            ("Média por minuto", f"{filtered['eventos'].sum() / n_minutes:,.1f}".replace(",", "X").replace(".", ",").replace("X", ".")),
            ("Wikis com eventos", h.fmt_int(filtered["wiki"].nunique())),
        ])
        series = (filtered.pivot_table(index="janela_inicio", columns="wiki", values="eventos", aggfunc="sum", fill_value=0)
                  .reindex(axis_minutes, fill_value=0))  # minuto sem evento no filtro vale 0, não some da série
        fig = go.Figure()
        for wiki in [w for w in WIKI_ORDER if w in series.columns]:  # cor fixa por wiki, filtro não repinta
            fig.add_trace(go.Scatter(x=series.index, y=series[wiki], mode="lines+markers", name=wiki,
                                     line=dict(color=WIKI_COLORS[wiki])))
        fig.update_layout(title="Eventos por minuto nas wikis escolhidas", yaxis_title="eventos", xaxis_title=None,
                          xaxis=dict(tickformat="%H:%M", dtick=60_000))
        show(fig, key="demo_serie")
        table = (filtered.groupby(["wiki", "tipo_editor", "tipo_evento"], as_index=False)["eventos"].sum()
                 .sort_values("eventos", ascending=False))
        h.show_table(table, height=320)
        st.caption("Da 9ª wiki em diante na ordem do período, a cor é cinza neutro (paleta categórica de 8 cores).")
