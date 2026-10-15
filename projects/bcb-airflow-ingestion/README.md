# Ingestão incremental de séries do Banco Central com Airflow

Uma DAG diária que baixa séries do SGS (Banco Central), faz só a carga incremental, valida cada série e consolida
tudo em uma tabela gold versionada. O app mostra o resultado; o pipeline roda localmente.

## Problema

Séries macroeconômicas (Selic, câmbio, inflação) mudam todo dia útil ou todo mês. Atualizar à mão é frágil, baixar tudo
a cada execução é desperdício, e gravar dado quebrado contamina tudo o que depende dele. A pergunta do projeto é:
como manter essas séries atualizadas de forma automática, idempotente e com validação antes da camada final?

## Dados

- **Fonte:** API SGS do Banco Central, dados públicos sem autenticação
  (`https://api.bcb.gov.br/dados/serie/bcdata.sgs.{código}/dados`). Real, não sintético.
- **Séries (códigos validados na API):**

| Código SGS | Nome no projeto | Descrição | Periodicidade |
|---|---|---|---|
| 432 | `selic_meta` | Meta da Selic definida pelo Copom | diária (muda nas reuniões) |
| 1 | `dolar_venda` | Dólar americano, venda | diária (dias úteis) |
| 433 | `ipca_mensal` | IPCA, variação mensal | mensal |
| 4189 | `selic_over_mensal` | Selic over, média mensal | mensal |
| 189 | `igpm_mensal` | IGP-M, variação mensal | mensal |

- **Janelas:** a API aceita no máximo 10 anos por consulta em séries diárias. O cliente usa janelas de 5 anos.
- **Identificação:** `User-Agent: portfolio-demo/0.1 (https://github.com/larissamagistrali)`.
- **Licença/uso:** dados públicos do BCB; cite "Fonte: Banco Central do Brasil, SGS".

## Abordagem

```
start -> fetch[série] (mapeado) -> validate[série] (mapeado) -> write -> end
```

- **Pacote puro** `src/bcb_ingestion/`: `client.py` (URL, janelas, retry com backoff em 429/5xx/rede, parse),
  `incremental.py` (carga a partir da última data), `validate.py`, `storage.py` (escrita atômica e idempotente) e
  `pipeline.py` (uma função por tarefa da DAG). Nada disso depende do Airflow.
- **DAG** `dags/bcb_ingestion_dag.py`: TaskFlow API, `fetch.expand(codigo=...)` (dynamic task mapping), `retries=2`,
  `schedule="0 9 * * *"`, `catchup=False`, `max_active_runs=1`. Só orquestra as funções do pacote.
- **Camadas** (em `data/raw/`, gitignored, e na saída em `data/`):
  - `bronze/`: resposta JSON bruta de cada janela.
  - `silver/<código>.parquet`: histórico por série, uma linha por `(codigo, data)`.
  - `series_gold.parquet` (raiz de `data/`): tabela longa `data, serie, codigo, valor`.
  - `series_status.json`: primeira/última data, contagem de linhas e linhas novas na última execução por série.
  - `runs.json`: status das execuções do Airflow (estado, tarefas, duração), extraído do metastore.
- **Incremental:** a coleta começa na última data da silver, inclusive, para capturar revisões do BCB. Reexecutar
  com os mesmos dados não duplica nada (testado).
- **Validação:** datas nulas, valores nulos ou não numéricos, datas duplicadas por série e datas futuras
  falham a tarefa `validate` da série. Nesse caso o `write` não roda.

## Resultados

Números copiados dos arquivos gerados pela execução real (`data/series_status.json` e `data/runs.json`).

| Série | Primeira data | Última data | Linhas |
|---|---|---|---|
| selic_meta | 2010-01-01 | 2026-10-14 | 6.131 |
| dolar_venda | 2010-01-04 | 2026-10-14 | 4.215 |
| ipca_mensal | 2010-01-01 | 2026-09-01 | 201 |
| selic_over_mensal | 2010-01-01 | 2026-10-01 | 202 |
| igpm_mensal | 2010-01-01 | 2026-09-01 | 201 |

Total na gold: **10.950 linhas**, 0 duplicatas, 0 valores nulos.

Execuções registradas (`airflow dags test`, Airflow 3.3.2, sqlite, executor local padrão):

| Execução | Data lógica | Estado | Tarefas com sucesso | Duração |
|---|---|---|---|---|
| 1ª (carga inicial, 20 chamadas à API) | 2026-10-13 | success | 13/13 | 63,4 s |
| 2ª (incremental, 5 chamadas à API) | 2026-10-14 | success | 13/13 | 3,6 s |

A duração é de início ao fim das tarefas; o `dags test` completo levou cerca de 67 s, incluindo o carregamento do Airflow.
Na 2ª execução, a série de status mostra `linhas_novas_na_execucao = 0`: nenhuma linha nova era esperada no mesmo dia.

## Como rodar

Pré-requisitos: Python 3.11.

```bash
cd projects/bcb-airflow-ingestion
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt -c https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.11.txt

# Testes (HTTP mockado; não acessa a API)
PYTHONPATH=src pytest tests -q

# Pipeline sem Airflow (mesma lógica da DAG, sequencial)
PYTHONPATH=src python -c "from datetime import date; from pathlib import Path; from bcb_ingestion.pipeline import run_all; print(run_all(Path('data'), date.today()))"

# Airflow local: DagBag + execução de uma data lógica, com AIRFLOW_HOME FORA do repositório
export AIRFLOW_HOME=$HOME/airflow-bcb AIRFLOW__CORE__LOAD_EXAMPLES=False \
  AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags PYTHONPATH=$PWD/src BCB_DATA_DIR=$PWD/data
airflow db migrate
airflow dags test bcb_ingestion 2026-10-13
```

Com Docker: `docker compose up airflow-init` e depois `docker compose up -d`; UI em `http://localhost:8611`.
**Este caminho não foi executado** (ver Limitações).

Para o app, na raiz do repositório: `streamlit run app/Home.py`. A página 3 lê só os arquivos de `data/`.

## Limitações

**Verificado aqui:**
- `pytest` do projeto: 20 testes passando com o Airflow 3.3.2 instalado (HTTP mockado).
- DagBag do Airflow carrega `bcb_ingestion` sem erros de importação.
- `airflow dags test bcb_ingestion` executou duas vezes com dados reais da API (ver Resultados). As 13 tarefas terminaram em sucesso.
- `runs.json` foi extraído do metastore sqlite (tabelas `dag_run` e `task_instance`) com um script de stdlib, fora do repositório.
- A página do app renderiza sem exceções no `AppTest` do Streamlit.

**Não verificado:**
- **docker-compose não foi executado.** O daemon do Docker estava desligado. O arquivo foi lido como YAML válido, e a imagem e o `_PIP_ADDITIONAL_REQUIREMENTS` não foram testados.
- **Execução contínua por scheduler** não foi observada. As execuções foram manuais, com `dags test` (in-process).
- **Grafo da DAG** na página é DOT gerado à mão. Não houve renderização com o binário `dot` (não instalado aqui); a renderização depende do `st.graphviz_chart` no navegador.
- **Demo interativa** foi checada só no nível de renderização. A interação não foi testada com cliques.
- Python 3.12 e versões mais novas de bibliotecas não foram testados.

**Limitações de desenho:**
- O estado incremental fica em `data/raw/silver`, que é gitignored. Se a pasta for apagada, a próxima execução faz a carga completa desde 2010 (cerca de 1 minuto).
- A validação é tudo-ou-nada: uma série inválida falha o `validate` dela e faz o `write` ser pulado (`upstream_failed`), inclusive para as outras séries.
- As séries mensais têm defasagem própria do BCB (última data 01/09/2026 para IPCA e IGP-M). Não há tratamento de feriados nem de janelas de revisão além da última data.
- O índice base 100 usa a primeira observação de cada série como referência. Isso muda conforme o período escolhido.
  Só entram níveis comparáveis: o câmbio direto e as variações mensais (IPCA, IGP-M) como índice acumulado, Π(1 + r/100).
  As taxas (Selic meta e Selic acumulada no mês, ambas em % a.a.) ficam fora e aparecem em p.p.: base 100 de uma taxa
  mede a variação da própria taxa. A natureza de cada série está em `src/bcb_ingestion/indices.py`, com testes.
