# Qualidade de dados em contratos públicos (PNCP)

Framework próprio de validação de dados, em Python puro (pandas), sem Great Expectations. Aplica regras declarativas
com severidade (erro ou aviso) e dimensões (completude, unicidade, validade, consistência, atualidade) a uma amostra de
**contratos reais publicados no PNCP**, e gera um relatório JSON e uma amostra das linhas que falharam.

## Problema

Dados públicos de contratação chegam com erros típicos: campos vazios, CNPJ ou CPF digitados errado, datas
incoerentes, valores negativos ou extremos e registros repetidos. O objetivo é medir esses problemas com regras
explícitas e reproduzíveis, em vez de descobri-los depois, num relatório analítico.

## Dados

- **Fonte:** PNCP, Portal Nacional de Contratações Públicas. API pública de consulta, sem autenticação.
- **Endpoint:** `GET https://pncp.gov.br/api/consulta/v1/contratos`, consulta por data de publicação, paginada.
  Parâmetros usados: `dataInicial=20250101`, `dataFinal=20250331`, `pagina`, `tamanhoPagina=500`.
- **Janela:** contratos publicados de 2025-01-01 a 2025-03-31. A API informa 412.779 registros e 826 páginas nesse
  período.
- **Amostra:** 12 páginas sorteadas entre as 826 com semente fixa (42), totalizando **6.000 registros**. É amostragem
  por páginas (cluster), não por linhas isoladas. Os números descrevem essa amostra, não o total do PNCP.
- **Data de coleta:** 2026-10-14. Os valores de `data_referencia` e de atualidade usam essa data.
- **Tipo:** dados **reais**, sem síntese.
- **Licença e citação:** dados públicos de acesso livre do PNCP. Cite a fonte: "PNCP, Portal Nacional de Contratações
  Públicas, API de consulta de contratos".
- **Dados pessoais:** registros de pessoa física (e registros marcados como PJ com número no formato de CPF) têm CPF
  e nome ocultados antes de qualquer exportação ou exibição. A validação de CPF roda sobre o valor original, em memória.

Campos usados (ver `src/data_quality/normalizar.py`): número de controle PNCP, retificação, CNPJ do órgão, tipo de
pessoa, CPF/CNPJ do fornecedor, nome do fornecedor, valores (inicial, parcela, global), datas (assinatura, vigência
início e fim, publicação, atualização), UF e código IBGE da unidade e objeto do contrato.

## Abordagem

- **Regras declarativas** (`src/data_quality/regras.py`): cada regra tem id, descrição, dimensão, severidade e uma
  função que devolve duas máscaras (avaliável, falha). Linhas que não se aplicam não contam como falha. Regra cujas
  colunas não existem é marcada como *ignorada*, nunca como falha.
- **Catálogo para contratos** (`src/data_quality/catalogo.py`): 25 regras.
- **Validadores** (`src/data_quality/validadores.py`): CNPJ e CPF pelo dígito verificador, não só pelo tamanho.
- **Valores extremos:** IQR com k=3 sobre o valor; e escore z modificado por MAD (Iglewicz-Hoaglin, limite 3,5) na
  escala log10, porque valores de contrato são muito assimétricos.
- **Referencial:** UF no conjunto das 27 UFs; os dois primeiros dígitos do código IBGE do município precisam bater
  com a UF informada.
- **Score:** por regra, taxa de aprovação = 1 − falhas/avaliadas. Por dimensão, média dessas taxas. Geral, média das
  regras. Também reporto "registros sem erro" (linhas sem nenhuma regra de severidade erro).
- **Relatório e amostra:** `data/relatorio_qualidade.json` (por regra: avaliadas, falhas, %, severidade, dimensão,
  status e até 5 exemplos) e `data/linhas_com_falha.parquet` (até 300 linhas com falha, todas as de erro primeiro, com
  pessoa física mascarada, zstd).
- **Demo no app:** o visitante envia um CSV (até 1 MB e 2.000 linhas) ou cola o texto, escolhe as regras e vê o
  resultado com o mesmo código (`data_quality`). Nada é salvo.

### Resultados (amostra de 6.000 contratos, copiados de `data/relatorio_qualidade.json`)

| Dimensão | Score (% de aprovação) | Regras que passaram |
|---|---|---|
| Completude | 100,00% | 5 de 5 |
| Unicidade | 100,00% | 2 de 2 |
| Validade | 98,94% | 8 de 12 |
| Consistência | 99,98% | 4 de 5 |
| Atualidade | 100,00% | 1 de 1 |

- **Score geral:** 99,49%. Registros sem erro: 99,95% (3 de 6.000 com alguma regra de severidade erro).
- **Regras:** 25 avaliadas, 20 passaram, 5 falharam.
- **Registros com aviso ou erro:** 728 (12,1%), quase todos por valor extremo (V011).

Regras que falharam:

| ID | Regra | Severidade | Falhas | % |
|---|---|---|---|---|
| V003 | CNPJ do fornecedor (pessoa jurídica) com dígito verificador válido | erro | 2 de 5.670 | 0,04% |
| V004 | CPF do fornecedor (pessoa física) com dígito verificador válido | erro | 1 de 268 | 0,37% |
| V011 | `valor_global` sem valor extremo (IQR, k=3) | aviso | 714 de 6.000 | 11,90% |
| V012 | `valor_global` sem valor extremo (MAD, escala log10) | aviso | 21 de 5.955 | 0,35% |
| K004 | publicação não é anterior à assinatura | aviso | 7 de 6.000 | 0,12% |

Leitura dos achados:

- **V003:** dois registros marcados como pessoa jurídica têm número com 11 dígitos (formato de CPF). O nome
  correspondente parece de pessoa física; pode ser erro de classificação no próprio PNCP ou do órgão.
- **V004:** um registro de pessoa física tem número com 14 dígitos (formato de CNPJ).
- **V011:** IQR com k=3 sobre valores brutos marca como extremos contratos de alto valor legítimos. É um alerta de
  investigação, não erro. Por isso a regra é de severidade aviso, e a versão em log (V012) é mais específica.
- **K004:** a publicação ocorreu antes da assinatura em 7 casos. Pode ser correção posterior ou erro de data.
- **A001 (atualidade)** passou em todos os registros. Com janela de 730 dias, a regra não separa nada nesta amostra,
  e por isso não deve ser lida como evidência de que a atualização é boa.

Para os números: o relatório completo fica em `data/relatorio_qualidade.json` e a amostra de falhas em
`data/linhas_com_falha.parquet`. Os dois juntos têm cerca de 52 KB.

## Como rodar

Requer Python 3.11+ e as dependências de `requirements.txt` deste projeto.

```bash
cd projects/data-quality
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
PYTHONPATH=src .venv/bin/python -m data_quality.pipeline --coletar   # consulta a API do PNCP (precisa de rede)
PYTHONPATH=src .venv/bin/python -m data_quality.pipeline             # só valida o cache já coletado
.venv/bin/python -m pytest tests -q                                  # testes
```

- O `--coletar` grava as páginas JSON em `data/raw/pncp/` (ignorado pelo Git, pode ser apagado depois). Sem
  `--coletar`, o pipeline usa esse cache e falha com mensagem clara se ele não existir.
- As páginas são reutilizadas se já existirem. A pausa entre requisições é de 1 segundo, e o User-Agent é
  `portfolio-demo/0.1 (https://github.com/larissamagistrali)`.
- A saída vai para `data/` (`--saida` altera o destino). A página Streamlit lê só esses dois arquivos.

## Limitações

- **Amostra.** 6.000 registros de 12 páginas de uma janela de três meses. Não representa o PNCP inteiro, nem outros
  períodos. As taxas de falha são estimativas da amostra.
- **Sem DuckDB.** O motor usa pandas. A validação roda em memória, e o volume da amostra cabe folgado.
- **Modalidade de licitação não coberta.** O endpoint de contratos não traz modalidade. O domínio aplicado é o de
  tipo de pessoa (PF/PJ/PE) e o de UF. Validar modalidade exigiria o endpoint de contratações.
- **Atualidade.** Mede a data de atualização contra a data de extração, com janela de 730 dias. Não mede frescor
  em relação ao momento da publicação no PNCP, e nesta amostra não discrimina nada (ver acima).
- **Valores extremos.** IQR e MAD são critérios estatísticos. Muitos extremos são legítimos (contratos grandes), e a
  regra só sinaliza para investigação.
- **CNPJ alfanumérico** (formato previsto para o futuro) não é validado; só o formato numérico.
- **Dados pessoais.** CPF e nome de pessoa física são ocultados na exportação e na tela. A regra V004 foi avaliada
  em memória e não é reproduzível a partir do parquet exportado.
- **Demo no app.** Valida o formato e as regras, não a veracidade do conteúdo. O modelo de CSV tem dados fictícios.
- **Não verificado aqui:** a reexecução de `--coletar` com o diretório `data/raw/pncp/` vazio. A coleta real foi feita
  em 2026-10-14 para um cache fora do repositório, e o pipeline foi rodado apontando para esse cache. Mudanças posteriores
  na API não foram testadas.
