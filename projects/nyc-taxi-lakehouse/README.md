# NYC Taxi · Lakehouse com PySpark

## Problema
Quando e onde a demanda de táxis amarelos de Nova York se concentra, e quanto cada hora, dia e zona gera em receita e gorjeta?
Para responder isso com confiança, o dado bruto precisa de limpeza, validação e agregação, e cada número precisa ser rastreável até
a linha de origem. Este projeto implementa essa cadeia em três camadas (bronze, silver e gold) com PySpark, medindo quantas linhas
cada regra de qualidade descarta.

## Dados
- **Tipo: REAL** (rótulo `real` no app). Dois meses de corridas de táxi amarelo da NYC Taxi & Limousine Commission (TLC):
  `yellow_tripdata_2024-01.parquet` e `yellow_tripdata_2024-02.parquet`, mais a tabela de zonas `taxi_zone_lookup.csv`.
- Fonte: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). Downloads diretos usados:
  `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet` (e `-02`) e
  `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`.
- Licença: dados públicos da TLC. Use com atribuição à fonte e consulte os termos de uso publicados pela TLC.
- Volume bruto: **5.972.150 viagens** (2.964.624 em janeiro e 3.007.526 em fevereiro). Os dois arquivos têm o mesmo schema de 19 colunas.
- Os arquivos brutos **não** estão no repositório (`data/raw/` é ignorado pelo Git). O pipeline espera que eles estejam em `data/raw/tlc/`.

## Abordagem
1. **Bronze** (`bronze.py`): lê cada Parquet mensal sem alterar valores e acrescenta `source_file`, `source_month` e `ingested_at`.
   Grava particionado por `source_month`.
2. **Silver** (`silver.py`):
   - Tipagem explícita (timestamps, inteiros, decimais) e seleção das colunas usadas.
   - Regras de qualidade aplicadas em ordem. Cada linha rejeitada conta só na primeira regra que viola:

     | Regra | Condição de rejeição |
     |---|---|
     | `null_timestamp` | pickup ou dropoff nulo |
     | `dropoff_before_pickup` | dropoff anterior ao pickup |
     | `outside_source_month` | pickup fora do mês do arquivo de origem |
     | `invalid_distance` | distância nula, zero ou acima de 500 milhas |
     | `invalid_fare` | tarifa nula, zero ou acima de US$ 1.000 |
     | `invalid_total` | valor total nulo ou negativo |
     | `invalid_passengers` | passageiros nulos, zero ou acima de 6 |

   - Deduplicação de linhas idênticas em todas as colunas da viagem.
   - Enriquecimento com zona e bairro de embarque e desembarque (junção com broadcast), e colunas derivadas: data, hora, dia da semana, duração em minutos.
   - Invariante verificado no código: `bronze = silver + rejeitadas por regra + duplicadas`. Se não fechar, o pipeline para.
   - Particionado por `pickup_month`.
3. **Gold** (`gold.py`): tabelas pequenas exportadas para `data/` em Parquet zstd. As somas (viagens, receita, tarifa, gorjeta)
   são reagregáveis; a gorjeta média é calculada como **soma das gorjetas / soma das tarifas** (média ponderada), não média das razões.
   - `gold_overview`: totais do período.
   - `gold_daily`, `gold_hourly`: viagens e receita por dia e por hora.
   - `gold_heatmap_hour_dow`: viagens por dia da semana e hora (base do mapa de calor).
   - `gold_zone`, `gold_zone_hour`: por zona de embarque, e por zona × hora (base da demonstração interativa).
   - `gold_od_top`: 20 pares origem-destino mais frequentes.
   - `gold_distributions`: histogramas de duração (faixas de 5 min, até 60+) e distância (faixas de 0,5 mi, até 20+).
4. **Relatório de qualidade** (`data/quality_report.json`): totais, contagem por regra com descrição, contagem por mês de origem,
   linhas de cada tabela gold, tempos de execução e versões de Spark, Java e Python.

Diagrama: `arquivos brutos → bronze → silver → gold → app`. O app lê só o gold e o relatório.

## Resultados
Números copiados de `data/quality_report.json` e das tabelas gold de uma execução real (Spark 4.2.0, Java 17.0.10, Python 3.11.9, `local[4]`, driver com 3 GB).

**Funil bronze → silver**

| Etapa | Linhas |
|---|---:|
| Bronze (bruto, jan + fev) | 5.972.150 |
| Rejeitadas: `null_timestamp` | 0 |
| Rejeitadas: `dropoff_before_pickup` | 63 |
| Rejeitadas: `outside_source_month` | 33 |
| Rejeitadas: `invalid_distance` | 128.662 |
| Rejeitadas: `invalid_fare` | 72.368 |
| Rejeitadas: `invalid_total` | 0 |
| Rejeitadas: `invalid_passengers` | 327.643 |
| Duplicadas | 1 |
| **Silver (válido)** | **5.443.380 (91,15%)** |

Por mês: janeiro 2.964.624 → 2.723.730 na silver; fevereiro 3.007.526 → 2.719.650. Zonas de embarque sem correspondência no lookup: 0.

**Indicadores (gold_overview, período de 1/1/2024 a 29/2/2024)**

| Métrica | Valor |
|---|---:|
| Viagens válidas | 5.443.380 |
| Receita total (`total_amount`) | US$ 149.131.392,39 |
| Tarifa média | US$ 18,41 |
| Gorjeta sobre tarifa (ponderada) | 18,92% |
| Duração média | 15,9 min |
| Distância média | 3,255 mi |
| Zonas de embarque com viagens | 258 |

- Pico de demanda por hora: 18h (398.100 viagens, somando os dois meses). Menor: 4h (25.757).
- Zona de embarque com mais viagens: Midtown Center (272.045), seguida de Upper East Side South (268.085) e JFK Airport (256.438).
- Viagens por dia: entre 62.402 e 112.761.

**Tempo de execução (última execução)**: bronze 5,4 s, silver 27,9 s, gold 7,6 s, total 40,9 s.

Esses números descrevem os dados da TLC para jan–fev/2024 e mostram o pipeline funcionando. Não são uma análise completa do mercado de táxis de NYC.

## Como rodar
Pré-requisitos: Python 3.11+ e **Java 17** (Spark 4 exige Java 17 ou superior).

```bash
# 1. Ambiente do pipeline (separado do app)
python3 -m venv .venv-nyc
.venv-nyc/bin/pip install -r projects/nyc-taxi-lakehouse/requirements.txt

# 2. Arquivos brutos (gitignored) em projects/nyc-taxi-lakehouse/data/raw/tlc/
#    yellow_tripdata_2024-01.parquet, yellow_tripdata_2024-02.parquet, taxi_zone_lookup.csv
#    (URLs na seção Dados). Se faltar algum, o pipeline lista o que baixar.

# 3. Pipeline completo (bronze -> silver -> gold, grava data/*.parquet e data/quality_report.json)
PYTHONPATH=projects/nyc-taxi-lakehouse/src .venv-nyc/bin/python -m nyc_taxi_lakehouse.pipeline

# 4. Testes (regras de qualidade, deduplicação, enriquecimento e gold)
.venv-nyc/bin/pytest projects/nyc-taxi-lakehouse/tests -q
```

O app lê `data/` e não precisa de Spark. Para vê-lo: `make app` na raiz do repositório.

## Limitações
- **Parquet, não Delta Lake.** O `delta-spark` não foi instalado nem testado com o Spark 4.2.0 deste ambiente, então não há
  transações ACID nem time travel. O particionamento e a estrutura de camadas são os mesmos e podem migrar para Delta.
- **Execução local, um nó.** Spark em `local[4]`. Não há cluster, streaming, orquestração (Airflow etc.) nem reprocessamento incremental:
  o pipeline recria as camadas inteiras a cada execução.
- **Sem Docker.** Não há Dockerfile. Tudo foi executado nativamente no macOS. O Docker daemon estava desligado.
- **Versões testadas**: PySpark 4.2.0 e pandas 3.0.6 (o PySpark emite um aviso de compatibilidade com pandas 3, que é filtrado no pipeline).
  O `requirements.txt` aceita `pyspark>=4.0,<5`, mas só essa versão foi executada.
- **Regras de qualidade são escolhas minhas**, não validadas contra a documentação da TLC: teto de 500 milhas, US$ 1.000 de tarifa e 1 a 6 passageiros.
  Os limites podem ser ajustados em `silver.py`.
- **Passageiros nulos.** A regra `invalid_passengers` descarta 327.643 linhas (cerca de 5,5% da bronze), a maior parte por campo nulo.
  Se a ausência de passageiros não for aleatória, isso pode enviesar análises que dependem desse campo. Nenhuma análise aqui depende dele.
- **Gorjeta.** Segundo o dicionário de dados da TLC (não reverificado aqui), gorjetas em dinheiro não são registradas. A "gorjeta sobre tarifa"
  reflete pagamentos com cartão e subestima a gorjeta real.
- **Fuso horário.** Os timestamps são tratados como horário local de NYC, sem conversão de fuso (sessão Spark em UTC). A hora do dia é a
  registrada no arquivo. Não há conversão para horário de verão.
- **Zonas.** Zonas de embarque e desembarque vêm do lookup da TLC. Zonas sem correspondência ficam com o rótulo "Zona não mapeada" (0 linhas nesta execução).
- **Sem dados de anos anteriores ou posteriores.** O período é jan–fev/2024. Sazonalidade e tendências não são estimadas.
