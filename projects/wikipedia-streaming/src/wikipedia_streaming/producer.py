"""Producer: lê o SSE da Wikimedia (uma conexão, sem paralelismo) e publica no Kafka.

- Chave da mensagem = wiki (ex.: `enwiki`); valor = JSON bruto do evento.
- Reconexão com backoff exponencial e cabeçalho `Last-Event-ID` (último `id:` recebido).
- Reenvios após reconexão são descartados pelo `meta.id` (deduplicação em memória).

Uso: PYTHONPATH=src python -m wikipedia_streaming.producer --duration 240 --stats-out stats.json
"""
from __future__ import annotations

import argparse
import json
import logging
import time
from collections import OrderedDict
from collections.abc import Iterator
from pathlib import Path

import requests

from . import config
from .sse import SSEEvent, parse_sse

log = logging.getLogger("producer")
DEDUP_WINDOW = 50_000  # quantos meta.id recentes lembrar para descartar reenvios


def kafka_config(bootstrap: str) -> dict[str, object]:
    """Configuração do produtor. Idempotência evita duplicatas nas retentativas do cliente."""
    return {
        "bootstrap.servers": bootstrap,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 50,
        "client.id": "wiki-recentchange-producer",
    }


class RecentIds:
    """Conjunto limitado de ids já publicados (para descartar reenvios após reconexão)."""

    def __init__(self, capacity: int = DEDUP_WINDOW) -> None:
        self.capacity = capacity
        self._seen: OrderedDict[str, None] = OrderedDict()

    def add_if_new(self, key: str) -> bool:
        if key in self._seen:
            return False
        self._seen[key] = None
        if len(self._seen) > self.capacity:
            self._seen.popitem(last=False)
        return True


def open_stream(url: str, last_event_id: str | None, timeout: tuple[float, float]) -> requests.Response:
    headers = {
        "User-Agent": config.USER_AGENT,
        "Accept": "text/event-stream",
    }
    if last_event_id:
        headers["Last-Event-ID"] = last_event_id
    resp = requests.get(url, headers=headers, stream=True, timeout=timeout)
    resp.raise_for_status()
    return resp


def iter_sse(resp: requests.Response) -> Iterator[SSEEvent]:
    yield from parse_sse(resp.iter_lines(decode_unicode=True, chunk_size=4096))


def backoff_delay(attempt: int) -> float:
    """1 s, 2 s, 4 s, ... até BACKOFF_MAX_SECONDS."""
    return min(config.BACKOFF_MAX_SECONDS, config.BACKOFF_BASE_SECONDS * (2 ** attempt))


class Stats:
    def __init__(self) -> None:
        self.received = 0  # eventos "message" lidos do SSE
        self.published = 0  # confirmados pelo broker (callback de entrega)
        self.delivery_failed = 0
        self.duplicates = 0  # reenvios descartados após reconexão
        self.invalid = 0  # JSON inválido ou sem wiki
        self.reconnects = 0

    def as_dict(self) -> dict[str, int]:
        return dict(self.__dict__)


def run(duration: float, bootstrap: str, stats_out: Path | None = None, url: str = config.STREAM_URL) -> Stats:
    from confluent_kafka import Producer  # import tardio: os testes não precisam do cliente

    producer = Producer(kafka_config(bootstrap))
    stats = Stats()
    seen = RecentIds()
    last_id: str | None = None
    attempt = 0
    deadline = time.monotonic() + duration

    def on_delivery(err, _msg) -> None:
        if err is None:
            stats.published += 1
        else:
            stats.delivery_failed += 1
            log.error("falha de entrega: %s", err)

    while time.monotonic() < deadline:
        try:
            resp = open_stream(url, last_id, timeout=(10.0, 60.0))
            log.info("conectado ao SSE (Last-Event-ID=%s)", last_id)
        except requests.RequestException as exc:
            delay = backoff_delay(attempt)
            attempt += 1
            stats.reconnects += 1
            log.warning("falha ao conectar (%s); nova tentativa em %.0f s", exc, delay)
            time.sleep(min(delay, max(0.0, deadline - time.monotonic())))
            continue

        with resp:
            try:
                for ev in iter_sse(resp):
                    if time.monotonic() >= deadline:
                        break
                    if ev.id is not None:
                        last_id = ev.id
                    if ev.event != "message":
                        continue
                    attempt = 0  # houve dado válido: zera o backoff
                    stats.received += 1
                    _publish(producer, ev.data, stats, seen, on_delivery)
                    producer.poll(0)
            except requests.RequestException as exc:
                log.warning("conexão SSE interrompida (%s)", exc)
            else:
                if time.monotonic() < deadline:
                    log.warning("servidor encerrou o fluxo SSE")
            if time.monotonic() >= deadline:
                break
            # Qualquer fim de conexão passa pelo backoff (zerado só quando chega dado válido).
            stats.reconnects += 1
            delay = backoff_delay(attempt)
            attempt += 1
            time.sleep(min(delay, max(0.0, deadline - time.monotonic())))

    producer.flush(30)
    if stats_out is not None:
        stats_out.parent.mkdir(parents=True, exist_ok=True)
        stats_out.write_text(json.dumps(stats.as_dict(), indent=2), encoding="utf-8")
    log.info("fim: %s", stats.as_dict())
    return stats


def _publish(producer, data: str, stats: Stats, seen: RecentIds, on_delivery) -> None:
    try:
        payload = json.loads(data)
        wiki = payload["wiki"]
        meta_id = payload["meta"]["id"]
    except (ValueError, KeyError, TypeError):
        stats.invalid += 1
        return
    if not seen.add_if_new(meta_id):
        stats.duplicates += 1
        return
    while True:
        try:
            producer.produce(config.TOPIC, key=wiki.encode(), value=data.encode(), on_delivery=on_delivery)
            return
        except BufferError:  # fila local cheia: espera o broker drenar
            producer.poll(0.1)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--duration", type=float, default=240.0, help="segundos de captura")
    ap.add_argument("--bootstrap", default=config.BOOTSTRAP_SERVERS)
    ap.add_argument("--stats-out", type=Path, default=None)
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run(args.duration, args.bootstrap, args.stats_out)


if __name__ == "__main__":
    main()
