# Wikipedia em tempo real: streaming com Kafka

Edições da Wikipedia chegam pelo **Wikimedia EventStreams** (SSE), passam por um **Kafka 4.3.1 em modo KRaft** e
viram agregados por minuto em **janelas tumbling com watermark**. A latência de ponta a ponta é medida evento a
evento. O app Streamlit (página 4) lê só o agregado versionado em `data/`.

> **Dados reais**: uma captura de cerca de 5 minutos do stream `recentchange`, em 14/10/2026, de 22:55:53 a 23:00:52 UTC,
> com Kafka rodando localmente. É um retrato curto, não uma série histórica.

## Problema

Como acompanhar, em tempo quase real, o volume e o perfil das edições da Wikipedia (quais wikis, quanto é de bot,
quais tipos de evento) sem consultar a API de minuto em minuto? E quanto tempo leva entre a edição acontecer e ela
aparecer no agregado?

## Dados

- **Fonte:** [Wikimedia EventStreams](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams_HTTP_Service),
  stream `recentchange` (o feed de Recent Changes do MediaWiki), endpoint `https://stream.wikimedia.org/v2/stream/recentchange`.
- **Termos e licença:** a página do serviço não declara uma licença própria para os eventos e remete aos
  [Termos de Uso da Wikimedia Foundation](https://foundation.wikimedia.org/wiki/Policy:Terms_of_Use). O serviço público
  é descrito como voltado a ferramentas externas de pequena escala.
- **Boas práticas seguidas:** User-Agent descritivo (`portfolio-demo/0.1 (https://github.com/larissamagistrali)`), conforme a
  [etiqueta da API](https://www.mediawiki.org/wiki/API:Etiquette). Uma única conexão, sem paralelismo, com reconexão com backoff
  exponencial e `Last-Event-ID`.
- **Campos usados:** `meta.dt` (timestamp do evento), `wiki`, `bot`, `type` e `meta.id` (deduplicação). Títulos, comentários
  e nomes de usuário passam pelo Kafka local, mas **não** são gravados em `data/`.

Artefatos versionados em `data/` (8.136 bytes no total):

| Arquivo | Conteúdo |
|---|---|
| `agregado_por_minuto.parquet` | 522 linhas: `janela_inicio`, `wiki`, `tipo_editor` (humano/bot), `tipo_evento`, `eventos`, `fechada`, `janela_parcial` |
| `metrics.json` | período capturado, contagens do producer e do agregador, latências (p50/p95/máx), histograma, offset do relógio |

## Abordagem

```
SSE (1 conexão) → producer.py → Kafka 4.3.1 KRaft (tópico wiki-recentchange, 3 partições, chave = wiki)
                → aggregator.py (janelas de 1 min, watermark de 2 min) → Parquet + metrics.json → app
```

| Módulo (`src/wikipedia_streaming/`) | Papel |
|---|---|
| `config.py` | bootstrap (`KAFKA_BOOTSTRAP`), tópico (`WIKI_TOPIC`), janela de 60 s, tolerância de 120 s, User-Agent |
| `sse.py` | parser SSE puro (`event:`, `data:` multilinha, `id:` persistente, comentários `:`). Descarta evento incompleto no fim do fluxo |
| `producer.py` | `requests` com `stream=True`; publica com chave = `wiki` e producer idempotente (`acks=all`); backoff de 1 s a 60 s; `Last-Event-ID`; descarta reenvios por `meta.id` |
| `windowing.py` | funções puras: janela tumbling, watermark, classificação de tardios, contagem por (wiki, editor, tipo) |
| `aggregator.py` | consome o tópico, aplica `windowing`, regrava o Parquet a cada janela fechada e mede as latências |

**Regras de janela**

- A janela é definida pelo timestamp do evento (`meta.dt`), não pelo horário de chegada.
- **Watermark = maior timestamp já visto − 120 s.** Um evento abaixo do watermark é **tardio**: entra na contagem
  `tardios_descartados` e não entra no agregado.
- Uma janela fecha quando o fim dela fica abaixo do watermark. Nesse momento o Parquet é regravado.
- **Fim do run:** sem eventos novos o watermark para de avançar. As janelas ainda abertas são gravadas com
  `fechada = false` e têm a latência medida à parte.
- **Bordas:** a primeira e a última janela não são cobertas por inteiro pela captura (`janela_parcial = true`).

**Medições**

- **Latência de ponta a ponta:** instante em que a janela do evento foi gravada no Parquet menos `meta.dt`.
- **Até o consumo:** instante em que o agregador leu o evento menos `meta.dt`.
- **Desordem observada:** maior timestamp já visto menos `meta.dt`, ou seja, o quanto cada evento chegou "atrasado".
- **Relógio:** antes do run, `sntp -t 5 time.apple.com` mediu o offset do relógio local, gravado em `metrics.json`.

## Resultados

Captura de 14/10/2026, de 22:55:53 a 23:00:52 UTC (298,81 s de consumo; producer com `--duration 300`). Todos os
números abaixo vêm de `data/metrics.json` e `data/agregado_por_minuto.parquet`.

**Contagens: nenhuma perda entre as etapas**

| Etapa | Eventos |
|---|---|
| Recebidos do SSE (producer) | 7.405 |
| Publicados no Kafka (confirmados pelo broker) | 7.405 (0 falhas de entrega) |
| Reenvios descartados / inválidos / reconexões | 0 / 0 / 0 |
| Consumidos do tópico (agregador) | 7.405 |
| Agregados | 7.405 |
| **Tardios descartados (watermark de 2 min)** | **0** |
| Ignorados (tipo fora de edit/new/log/categorize) | 0 |

**Latência**

| Medida | n | p50 | p95 | máx |
|---|---|---|---|---|
| Ponta a ponta, janelas fechadas pelo watermark | 2.990 | 147,84 s | 176,45 s | 180,12 s |
| Ponta a ponta, janelas encerradas no fim do run | 4.415 | 100,84 s | 172,23 s | 179,23 s |
| Até o consumo pelo agregador (relógio local, bruto) | 7.405 | −0,08 s | −0,02 s | 0,46 s |
| Desordem observada | 7.405 | 0,00 s | 0,02 s | 0,34 s |

- **Toda a latência de ponta a ponta das janelas fechadas pelo watermark ficou entre 120 e 195 s.** O histograma tem
  865, 792, 625, 707 e 1 eventos nas faixas 120–135, 135–150, 150–165, 165–180 e 180–195 s. Isso bate com a teoria: uma
  janela de 60 s só fecha cerca de 60 + 120 = 180 s depois do seu início. A latência é dominada pela **tolerância de
  atraso escolhida**, não pelo Kafka.
- A **desordem real foi de no máximo 0,34 s**, e nenhum evento chegou a 120 s de atraso. A tolerância de 2 min é, portanto,
  muito conservadora para esse fluxo: uma tolerância menor deixaria o agregado mais fresco. Esse trade-off **não** foi
  medido com outras tolerâncias.
- A latência até o consumo é negativa porque o relógio local estava **0,222752 s atrás** do NTP (offset medido pelo `sntp`).
  Somando o offset, e supondo o relógio da Wikimedia sincronizado, fica em torno de 0,14 s (p50) e 0,20 s (p95).

**Volume e perfil** (6 janelas de 1 min: 22:55 a 23:00 UTC)

- Janelas fechadas pelo watermark: 22:55 (parcial), 22:56 e 22:57. Encerradas no fim do run: 22:58, 22:59 e 23:00 (parcial).
- Nas 4 janelas completas (22:56 a 22:59), a média foi de 1.500,5 eventos/min, entre 1.308 e 1.743.
- **Bots:** 2.275 de 7.405 eventos (30,7%). **Humanos:** 5.130.
- **Tipos:** `edit` 3.669 · `categorize` 3.362 · `log` 273 · `new` 101.
- **74 wikis distintas.** Top 5: `commonswiki` 2.792 · `enwiki` 1.306 · `trwiki` 939 · `wikidatawiki` 785 · `fawiki` 302.
  As 3 maiores somam 68,0% do volume.

## Como rodar

Pré-requisitos: Python 3.11+, Java 17+ e o tarball oficial do Apache Kafka 4.x (aqui, `kafka_2.13-4.3.1.tgz`, com o
SHA-512 conferido contra o checksum publicado).

```bash
# 1) dependências do pipeline (venv próprio; o app não precisa delas)
python3 -m venv .venv-wiki
.venv-wiki/bin/pip install -r projects/wikipedia-streaming/requirements.txt

# 2) Kafka em KRaft, nó único, com dados e logs FORA do repositório
KAFKA=/caminho/para/kafka_2.13-4.3.1
RUN=/tmp/wiki-kafka && mkdir -p "$RUN"
cat > "$RUN/server.properties" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
controller.listener.names=CONTROLLER
listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093
advertised.listeners=PLAINTEXT://localhost:9092
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs=$RUN/data
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=false
EOF
"$KAFKA/bin/kafka-storage.sh" format -t "$("$KAFKA/bin/kafka-storage.sh" random-uuid)" -c "$RUN/server.properties"
LOG_DIR="$RUN/logs" KAFKA_HEAP_OPTS="-Xms256m -Xmx768m" "$KAFKA/bin/kafka-server-start.sh" "$RUN/server.properties" &
"$KAFKA/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --create --topic wiki-recentchange --partitions 3 --replication-factor 1

# 3) agregador primeiro, depois o producer (cerca de 5 min de tráfego real). O agregado vai para projects/wikipedia-streaming/data/
export PYTHONPATH=projects/wikipedia-streaming/src
OFFSET=$(sntp -t 5 time.apple.com 2>/dev/null | tail -1 | awk '{print $1}')   # opcional (macOS): offset do relógio
.venv-wiki/bin/python -m wikipedia_streaming.aggregator --run-seconds 310 \
    --producer-stats /tmp/wiki-producer-stats.json ${OFFSET:+--clock-offset-s "$OFFSET"} &
sleep 4
.venv-wiki/bin/python -m wikipedia_streaming.producer --duration 300 --stats-out /tmp/wiki-producer-stats.json
wait

# 4) encerrar o Kafka e apagar dados e logs dele
"$KAFKA/bin/kafka-server-stop.sh" && rm -rf "$RUN"

# testes (sem rede e sem Kafka; o teste do cliente Kafka é pulado se confluent-kafka não estiver instalado)
.venv-wiki/bin/python -m pytest projects/wikipedia-streaming/tests -q
```

O agregador usa um consumer group novo a cada execução e lê o tópico desde o início. Para não misturar execuções,
recrie o tópico antes de uma nova captura.

**docker-compose (alternativa, não executada aqui):** `docker compose up` na pasta do projeto sobe `apache/kafka:4.3.1`
em KRaft, um serviço `kafka-init` que cria o tópico e os serviços `aggregator` e `producer` (`python:3.11-slim`, que
instalam as dependências ao iniciar). O agregado é gravado em `./data`.

## Limitações

- **Uma captura de cerca de 5 minutos.** É um retrato, não uma série. O volume por wiki oscila muito entre janelas
  curtas: numa primeira captura descartada (ver abaixo), `wikidatawiki` teve 2.314 eventos em 5 min; nesta, 785.
- **Captura descartada:** houve uma primeira captura (14/10/2026, 22:48:14 a 22:53:14 UTC, 9.176 eventos, 0 tardios) feita com
  uma versão anterior do agregador. Ela gravava em lotes de 30 s e misturava as latências das janelas fechadas pelo watermark
  com as das encerradas no fim do run. O agregador foi corrigido, a captura refeita e os artefatos em `data/` são só da segunda.
- **docker-compose escrito, mas NÃO executado:** o Docker daemon estava desligado. O arquivo passou em `docker compose config`
  (validação de sintaxe, que não precisa do daemon) e a tag `apache/kafka:4.3.1` existe no Docker Hub, mas os containers nunca
  rodaram. Todo o pipeline foi verificado nativamente: Kafka 4.3.1 do tarball oficial,
  Java 17.0.10, Python 3.11.9, confluent-kafka 2.16.0 (librdkafka 2.16.0).
- **Reconexão não exercitada com falha real:** em 5 min houve 0 reconexões. Segundo a documentação, o servidor corta conexões
  após 15 min. O backoff e a deduplicação têm testes unitários, mas a retomada via `Last-Event-ID` não foi observada na prática.
- **Watermark não exercitado pelo dado real:** 0 eventos tardios. A classificação de tardios e o fechamento de janelas foram
  verificados com eventos sintéticos nos testes (`tests/test_windowing.py`).
- **Metade das janelas fechou no fim do run, não pelo watermark** (3 de 6), porque o watermark só avança com eventos novos.
  As contagens delas estão completas até o fim da captura, e a latência delas é reportada separadamente. A primeira e a
  última janela são parciais.
- **Latência depende de relógios diferentes** (servidor da Wikimedia e máquina local). O offset local foi medido com `sntp`
  contra `time.apple.com` (+0,22 s); a correção supõe que o relógio da Wikimedia está sincronizado.
- **Entrega e estado:** um broker só, fator de replicação 1, sem TLS e sem autenticação. O estado das janelas fica em memória e
  os offsets não são commitados, então o agregador não retoma após uma falha. A deduplicação por `meta.id` vale só dentro do
  mesmo processo do producer (últimos 50.000 ids). Não há garantia exactly-once de ponta a ponta.
- **`bot`** é a marcação do próprio MediaWiki (conta com flag de bot), não uma detecção feita aqui.
