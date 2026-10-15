"""Página 5: validação de qualidade de dados em contratos públicos do PNCP (framework próprio, sem GX)."""
import io

import _bootstrap  # noqa: F401
import _helpers as h
import pandas as pd
import streamlit as st

from shared import data, ui

PROJECT = "data-quality"
MAX_BYTES = 1_000_000
MAX_LINHAS = 2000
DIMENSOES = {
    "completude": ("🧩", "Completude"),
    "unicidade": ("🔑", "Unicidade"),
    "validade": ("📏", "Validade"),
    "consistencia": ("🔗", "Consistência"),
    "atualidade": ("🕒", "Atualidade"),
}
EXEMPLO_CSV = """numero_controle_pncp,cnpj_orgao,tipo_pessoa,ni_fornecedor,nome_fornecedor,valor_global,data_assinatura,data_vigencia_inicio,data_vigencia_fim,data_publicacao,uf_sigla,codigo_ibge
00000000000001-1-000001/2025,11222333000181,PJ,11222333000181,EMPRESA EXEMPLO LTDA,15000.00,2025-02-01,2025-02-01,2025-12-31,2025-02-03,PR,4106902
00000000000001-1-000002/2025,11222333000181,PJ,11222333000182,EMPRESA EXEMPLO S.A.,8000.00,2025-02-05,2025-03-01,2025-02-28,2025-02-06,PR,4106902
00000000000001-1-000003/2025,11222333000181,PJ,11222333000181,EMPRESA EXEMPLO ME,-500.00,2025-03-10,2025-03-10,2026-03-10,2025-03-11,SP,4106902
00000000000001-1-000004/2025,11222333000181,PJ,11222333000181,EMPRESA EXEMPLO EPP,,2025-04-01,2025-04-01,2025-10-01,2025-04-02,PR,4106902
"""

ui.setup_page("Qualidade de dados", "🧪")
ui.header(
    "Qualidade de dados em contratos públicos",
    "Regras declarativas com severidade e dimensões, aplicadas a contratos reais do PNCP.",
    tags=["Python", "DuckDB/pandas", "pytest", "PNCP", "CNPJ/CPF"],
    code_path="projects/data-quality",
)
h.pipeline_note()

problema, dados, abordagem, resultado, demo = ui.tabs()

required = [data.data_path(PROJECT, f) for f in ("relatorio_qualidade.json", "linhas_com_falha.parquet")]
if h.missing_data(required, "Rode `python -m data_quality.pipeline` em projects/data-quality."):
    st.stop()

relatorio = data.read_json(str(data.data_path(PROJECT, "relatorio_qualidade.json")))
falhas = data.read_parquet(str(data.data_path(PROJECT, "linhas_com_falha.parquet")))
fonte = relatorio["fonte"]
regras = relatorio["regras"]


def pct(valor: float | None) -> str:
    return "n/d" if valor is None else f"{valor:.2f}%".replace(".", ",")


with problema:
    st.subheader("Qual problema isto resolve")
    st.markdown(
        """
Dados públicos de contratação são usados em decisões de gasto, auditoria e pesquisa, mas chegam com erros típicos:
campos obrigatórios vazios, CNPJ ou CPF digitados errado, datas que não fazem sentido, valores negativos ou
extremos e registros duplicados. Sem uma checagem explícita, esses problemas aparecem só depois, num relatório.

Este projeto transforma essas checagens em **regras declarativas**: cada uma tem um identificador, uma dimensão,
uma severidade (erro ou aviso) e uma taxa de falha medida nos dados reais. O resultado é um relatório auditável,
não uma opinião sobre o dado.
"""
    )

with dados:
    ui.data_notice(
        "real",
        "PNCP - Portal Nacional de Contratações Públicas (API pública de consulta, sem autenticação)",
        f"Endpoint `{fonte['endpoint']}`, contratos por data de publicação de "
        f"{fonte['periodo_publicacao'][0]} a {fonte['periodo_publicacao'][1]}. "
        f"Coleta em {fonte.get('coletado_em', 'n/d')}. Licença: dados públicos de acesso livre; citar o PNCP.",
    )
    ui.metric_row([
        ("Registros analisados", h.fmt_int(relatorio["registros"]), fonte["amostragem"]),
        ("Total na janela", h.fmt_int(fonte["total_registros_na_janela"]), "Contratos publicados no período no PNCP."),
        ("Regras", str(relatorio["regras_total"]), "Declarativas, com severidade e dimensão."),
    ])
    st.markdown(
        """
**Amostra, não censo.** O pipeline sorteia páginas (não linhas) da janela com semente fixa. Os números
descrevem a amostra e não o total do PNCP. Dados de pessoa física (CPF e nome) são ocultados antes de
qualquer exibição ou exportação; a validação de CPF roda sobre o valor original, em memória.
"""
    )
    st.caption("Campos usados: " + ", ".join(
        ["numero_controle_pncp", "cnpj_orgao", "tipo_pessoa", "ni_fornecedor", "valor_global",
         "data_assinatura", "data_vigencia_inicio", "data_vigencia_fim", "data_publicacao", "data_atualizacao",
         "uf_sigla", "codigo_ibge", "objeto_contrato"]))

with abordagem:
    st.subheader("Como as regras funcionam")
    st.markdown(
        """
- **Regra declarativa:** cada regra avalia uma coluna (ou um par de colunas) e marca as linhas que falham. Linhas
  que não se aplicam (ex.: CNPJ só para pessoa jurídica) não contam como falha.
- **Severidade:** `erro` reprova o registro para uso analítico; `aviso` sinaliza algo a investigar.
- **Dimensões:** completude, unicidade, validade, consistência e atualidade. O score de cada dimensão é a média
  das taxas de aprovação das regras que a compõem.
- **Documentos:** CNPJ e CPF são validados pelo dígito verificador, não só pelo tamanho.
- **Valores extremos:** IQR (k=3) sobre o valor, e MAD (escore z modificado) na escala log10, porque valores de
  contrato são muito assimétricos.
- **Referencial:** a UF precisa ser uma das 27 UFs, e os dois primeiros dígitos do código IBGE do município
  precisam bater com a UF.
"""
    )
    st.markdown(
        """
Código: `src/data_quality/` (`validadores.py`, `regras.py`, `catalogo.py`, `normalizar.py`, `coleta_pncp.py`,
`pipeline.py`). Testes em `tests/` (pytest). O app não recalcula nada: lê `data/relatorio_qualidade.json` e
`data/linhas_com_falha.parquet`, gerados pelo pipeline.
"""
    )

with resultado:
    st.subheader("Score por dimensão")
    dims = relatorio["dimensoes"]
    ui.metric_row([
        (f"{DIMENSOES[d][0]} {DIMENSOES[d][1]}",
         pct(dims[d]["score_pct"]),
         f"{dims[d]['regras_passaram']} de {dims[d]['regras_avaliadas']} regras passaram.")
        for d in DIMENSOES
    ])
    ui.metric_row([
        ("Score geral", pct(relatorio["score_geral_pct"]),
         "Média das taxas de aprovação de todas as regras (cada regra pesa igual)."),
        ("Registros sem erro", pct(relatorio["registros_sem_erro_pct"]),
         f"{h.fmt_int(relatorio['registros_com_erro'])} registro(s) com ao menos uma regra de severidade erro."),
        ("Regras que passaram", f"{relatorio['regras_passaram']} de {relatorio['regras_avaliadas']}",
         "Regras com zero falhas na amostra."),
    ])
    st.caption("O score geral é alto porque a maioria das regras de integridade passa. "
               "Os avisos (sobretudo valores extremos) mostram onde o dado merece investigação.")

    st.subheader("Regras")
    tabela = pd.DataFrame([
        {
            "ID": r["id"],
            "Regra": r["descricao"],
            "Dimensão": r["dimensao"],
            "Severidade": r["severidade"],
            "Avaliadas": h.fmt_int(r["avaliadas"]),
            "Falhas": h.fmt_int(r["falhas"]),
            "% falha": "" if r["pct_falha"] is None else f"{r['pct_falha']:.2f}".replace(".", ","),
            "Status": h.status_label("pass" if r["status"] == "passou" else "fail"),
        }
        for r in regras
    ])
    h.show_table(tabela)
    st.caption("Status com ícone e texto. Regras de severidade `erro` com falha indicam registros a corrigir ou excluir.")

    st.subheader("Exemplos de linhas que falharam")
    st.caption(f"Amostra de {len(falhas)} linhas (todas as de severidade erro, completadas por avisos, "
               "sorteadas com semente fixa). CPF e nome de pessoa física estão ocultos.")
    h.show_table(falhas.drop(columns=["numero_controle_pncp"], errors="ignore"), height=360)

    exemplos = [r for r in regras if r["exemplos"] and r["status"] == "falhou"]
    with st.expander("Valores de exemplo por regra"):
        for r in exemplos:
            st.markdown(f"**{r['id']}** · {r['descricao']}")
            h.show_table(pd.DataFrame(r["exemplos"]))

with demo:
    st.subheader("Valide o seu CSV")
    st.markdown(
        "Envie um CSV pequeno (até **1 MB** e **2.000 linhas**) ou cole o conteúdo, escolha as regras e veja o "
        "resultado. Usa as mesmas regras do pipeline (`data_quality`), aplicadas aos seus dados. Nada é salvo."
    )
    with st.expander("Colunas reconhecidas"):
        st.markdown(
            "As regras procuram estas colunas (com esses nomes). Coluna ausente faz a regra aparecer como "
            "**ignorada**, não como falha."
        )
        st.code(EXEMPLO_CSV.splitlines()[0], language="text")
        st.download_button("Baixar modelo com exemplo fictício", EXEMPLO_CSV, file_name="exemplo_contratos.csv",
                           mime="text/csv")
        st.caption("O modelo tem dados fictícios, apenas para mostrar o formato e algumas falhas.")

    modo = st.radio("Entrada", ["Enviar arquivo CSV", "Colar CSV"], horizontal=True, key="dq_modo")
    texto = None
    if modo == "Enviar arquivo CSV":
        arquivo = st.file_uploader("Arquivo CSV (até 1 MB)", type=["csv"], key="dq_upload")
        if arquivo is not None:
            if arquivo.size > MAX_BYTES:
                st.error(f"Arquivo com {arquivo.size / 1e6:.1f} MB; o limite é 1 MB.")
            else:
                try:
                    texto = arquivo.getvalue().decode("utf-8-sig")
                except UnicodeDecodeError:
                    st.error("Não consegui ler o arquivo como UTF-8. Salve o CSV com codificação UTF-8.")
    else:
        colado = st.text_area("CSV (cabeçalho na primeira linha)", height=180, key="dq_texto",
                              placeholder=EXEMPLO_CSV.splitlines()[0])
        if colado.strip():
            if len(colado.encode("utf-8")) > MAX_BYTES:
                st.error("Texto maior que 1 MB; reduza o conteúdo.")
            else:
                texto = colado

    nomes = {r["id"]: f"{r['id']} · {r['descricao']}" for r in regras}
    escolhidas = st.multiselect("Regras a aplicar", list(nomes), default=list(nomes),
                                format_func=lambda rid: nomes[rid], key="dq_regras")

    if texto is None or not texto.strip():
        st.info("Envie um arquivo ou cole um CSV para validar. Use o modelo acima para testar.")
    elif not escolhidas:
        st.warning("Escolha ao menos uma regra.")
    else:
        try:
            entrada = pd.read_csv(io.StringIO(texto), sep=None, engine="python", dtype=str)
        except (pd.errors.ParserError, ValueError) as erro:
            st.error(f"Não consegui interpretar o CSV: {erro}")
            entrada = None
        if entrada is not None and entrada.empty:
            st.info("O CSV não tem linhas de dados (só cabeçalho ou vazio).")
        elif entrada is not None and len(entrada) > MAX_LINHAS:
            st.error(f"{len(entrada)} linhas; o limite é {MAX_LINHAS}.")
        elif entrada is not None:
            data.use_project(PROJECT)
            from data_quality import regras as R  # noqa: E402
            from data_quality.catalogo import regras_contratos  # noqa: E402
            from data_quality.normalizar import mascarar_pessoa_fisica  # noqa: E402

            referencia = pd.Timestamp(relatorio["data_referencia"])
            catalogo = {r.id: r for r in regras_contratos(referencia)}
            res = R.avaliar(entrada, [catalogo[rid] for rid in escolhidas], mascarar=mascarar_pessoa_fisica)
            avaliadas = [r for r in res["regras"] if r["status"] != "ignorada"]
            com_falha = int(pd.DataFrame(res["falhas_por_regra"]).any(axis=1).sum()) if res["falhas_por_regra"] else 0
            ui.metric_row([
                ("Linhas recebidas", h.fmt_int(len(entrada))),
                ("Regras avaliadas", f"{len(avaliadas)} de {len(res['regras'])}",
                 "Regras ignoradas têm colunas ausentes."),
                ("Linhas com erro", h.fmt_int(int(res["linhas_com_erro"].sum()))),
                ("Linhas com aviso ou erro", h.fmt_int(com_falha)),
            ])
            tabela_demo = pd.DataFrame([
                {
                    "ID": r["id"],
                    "Regra": r["descricao"],
                    "Severidade": r["severidade"],
                    "Avaliadas": r["avaliadas"],
                    "Falhas": r["falhas"],
                    "Status": h.status_label(
                        {"passou": "pass", "falhou": "fail", "ignorada": "skipped"}[r["status"]]),
                    "Motivo": r["motivo"] or "",
                }
                for r in res["regras"]
            ])
            h.show_table(tabela_demo)
            exemplos_demo = [r for r in res["regras"] if r["status"] == "falhou" and r["exemplos"]]
            if exemplos_demo:
                with st.expander("Exemplos de valores que falharam"):
                    for r in exemplos_demo:
                        st.markdown(f"**{r['id']}** · {r['descricao']}")
                        h.show_table(pd.DataFrame(r["exemplos"]))
            else:
                st.success("Nenhuma regra selecionada falhou nestes dados.")
