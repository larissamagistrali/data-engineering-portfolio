"""Lógica do producer sem rede: backoff, deduplicação e publicação com cliente falso."""
import json

import pytest

from wikipedia_streaming import producer
from wikipedia_streaming.config import BACKOFF_MAX_SECONDS


def test_backoff_cresce_e_satura():
    assert producer.backoff_delay(0) == 1
    assert producer.backoff_delay(3) == 8
    assert producer.backoff_delay(50) == BACKOFF_MAX_SECONDS


def test_recent_ids_descarta_repetidos_e_limita_memoria():
    seen = producer.RecentIds(capacity=2)
    assert seen.add_if_new("a")
    assert not seen.add_if_new("a")
    assert seen.add_if_new("b")
    assert seen.add_if_new("c")  # expulsa "a"
    assert seen.add_if_new("a")


class FakeProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, key, value, on_delivery):
        self.sent.append((topic, key, value))


def _event(meta_id, wiki="enwiki"):
    return json.dumps({"wiki": wiki, "meta": {"id": meta_id}, "type": "edit"})


def test_publish_usa_wiki_como_chave_e_conta_invalidos_e_reenvios():
    fake = FakeProducer()
    stats = producer.Stats()
    seen = producer.RecentIds()
    cb = lambda err, msg: None  # noqa: E731
    producer._publish(fake, _event("m1", "dewiki"), stats, seen, cb)
    producer._publish(fake, _event("m1", "dewiki"), stats, seen, cb)  # reenvio após reconexão
    producer._publish(fake, "não é json", stats, seen, cb)
    producer._publish(fake, json.dumps({"meta": {}}), stats, seen, cb)  # sem wiki
    assert len(fake.sent) == 1
    topic, key, _ = fake.sent[0]
    assert key == b"dewiki"
    assert (stats.duplicates, stats.invalid) == (1, 2)


def test_kafka_config_sem_conexao():
    pytest.importorskip("confluent_kafka")
    from confluent_kafka import Producer

    cfg = producer.kafka_config("localhost:9092")
    assert cfg["enable.idempotence"] is True
    Producer(cfg)  # criar o cliente não abre conexão
