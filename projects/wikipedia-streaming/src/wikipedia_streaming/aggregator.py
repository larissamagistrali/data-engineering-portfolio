"""Agregador: consome o tópico, agrega em janelas de 1 min (watermark de 2 min) e grava Parquet.

Saídas em `--out-dir` (padrão: projects/wikipedia-streaming/data):
- agregado_por_minuto.parquet: janela_inicio, wiki, tipo_editor (humano|bot), tipo_evento, eventos,
  fechada (True = fechada pelo watermark; False = encerrada no fim do run, quando o watermark para de avançar)
  e janela_parcial (True = a captura não cobre o minuto inteiro: primeira e última janelas).
- metrics.json: período capturado, contagens (recebidos, publicados, tardios descartados...) e latências.

Latência ponta a ponta = instante em que a janela do evento foi gravada no Parquet - timestamp do evento
(meta.dt). O Parquet é regravado assim que uma ou mais janelas fecham. São medidas também a latência até o
consumo (instante de consumo - meta.dt) e a desordem observada (maior timestamp já visto - meta.dt).
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from . import config
from .windowing import ClosedWindow, Edit, TumblingWindows, aggregate, parse_epoch

log = logging.getLogger("aggregator")
PROJECT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_OUT = PROJECT_DIR / "data"
HIST_STEP = 15
HIST_EDGES = list(range(0, 301, HIST_STEP))  # segundos; o último bin agrupa o restante
AGG_FILE = "agregado_por_minuto.parquet"


def percentile(values: list[float], q: float) -> float | None:
    """Percentil por posto (nearest-rank). None se não há valores."""
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil(q * len(ordered)))
    return ordered[rank - 1]


def summary(values: list[float]) -> dict[str, float | int | None]:
    return {
        "n": len(values),
        "p50": _round(percentile(values, 0.50)),
        "p95": _round(percentile(values, 0.95)),
        "max": _round(max(values) if values else None),
    }


def histogram(values: list[float]) -> dict[str, list]:
    counts = [0] * len(HIST_EDGES)
    for v in values:
        idx = max(0, min(len(HIST_EDGES) - 1, int(v // HIST_STEP)))
        counts[idx] += 1
    labels = [f"{a}-{b}" for a, b in zip(HIST_EDGES, HIST_EDGES[1:])] + [f">={HIST_EDGES[-1]}"]
    return {"faixas_s": labels, "eventos": counts}


def _round(x: float | None) -> float | None:
    return None if x is None else round(x, 2)


def rows_for(window: ClosedWindow, closed_by_watermark: bool) -> list[dict]:
    start = datetime.fromtimestamp(window.start, tz=timezone.utc)
    counts: Counter = aggregate(window.edits)
    return [
        {
            "janela_inicio": start,
            "wiki": wiki,
            "tipo_editor": editor,
            "tipo_evento": event,
            "eventos": n,
            "fechada": closed_by_watermark,
        }
        for (wiki, editor, event), n in sorted(counts.items())
    ]


class Output:
    """Acumula as linhas agregadas, regrava o Parquet a cada fechamento e mede a latência de gravação."""

    COLUMNS = ["janela_inicio", "wiki", "tipo_editor", "tipo_evento", "eventos", "fechada"]

    def __init__(self, out_dir: Path, window_seconds: int = config.WINDOW_SECONDS) -> None:
        self.out_dir = out_dir
        self.window_seconds = window_seconds
        self.rows: list[dict] = []
        self.write_latency: dict[str, list[float]] = {"watermark": [], "fim_do_run": []}
        self.consume_latency: list[float] = []
        out_dir.mkdir(parents=True, exist_ok=True)

    def add_windows(self, windows: list[ClosedWindow], closed_by_watermark: bool, coverage: tuple[float, float]) -> None:
        if not windows:
            return
        for w in windows:
            self.rows.extend(rows_for(w, closed_by_watermark))
        self.persist(coverage)
        written_at = time.time()  # depois da escrita: é o "momento de gravação no agregado"
        kind = "watermark" if closed_by_watermark else "fim_do_run"
        for w in windows:
            for e in w.edits:
                self.write_latency[kind].append(written_at - e.ts)
                self.consume_latency.append(e.recv - e.ts)

    def persist(self, coverage: tuple[float, float]) -> None:
        first_ts, last_ts = coverage
        df = pd.DataFrame(self.rows, columns=self.COLUMNS)
        df["janela_inicio"] = pd.to_datetime(df["janela_inicio"], utc=True)
        df["eventos"] = df["eventos"].astype("int32")
        start = df["janela_inicio"].map(lambda t: t.timestamp())
        df["janela_parcial"] = (start < first_ts) | (start + self.window_seconds > last_ts)
        df.to_parquet(self.out_dir / AGG_FILE, compression="zstd", index=False)


def run(
    run_seconds: float,
    bootstrap: str,
    out_dir: Path,
    producer_stats: Path | None = None,
    clock_offset_s: float | None = None,
) -> dict:
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"{config.CONSUMER_GROUP}-{uuid.uuid4().hex[:8]}",  # grupo novo a cada run
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([config.TOPIC])
    windows = TumblingWindows()
    out = Output(out_dir)
    counts: Counter = Counter()
    disorder: list[float] = []
    first_recv = last_recv = min_ts = max_ts = None
    end = time.monotonic() + run_seconds
    hard_stop = end + 60  # drena o que restou no tópico, com teto

    try:
        while time.monotonic() < hard_stop:
            msg = consumer.poll(1.0)
            if msg is None:
                if time.monotonic() >= end:  # passou o tempo e não chegou mais nada: drenado
                    break
                continue
            if msg.error():
                log.error("erro do consumidor: %s", msg.error())
                continue
            recv = time.time()
            counts["consumed"] += 1
            try:
                payload = json.loads(msg.value())
                edit = Edit(
                    ts=parse_epoch(payload["meta"]["dt"]),
                    wiki=payload["wiki"],
                    bot=bool(payload.get("bot", False)),
                    event_type=payload["type"],
                    recv=recv,
                )
            except (ValueError, KeyError, TypeError):
                counts["invalid"] += 1
                continue
            first_recv = recv if first_recv is None else first_recv
            last_recv = recv
            min_ts = edit.ts if min_ts is None else min(min_ts, edit.ts)
            max_ts = edit.ts if max_ts is None else max(max_ts, edit.ts)
            if edit.event_type not in config.EVENT_TYPES:
                counts["ignored_type"] += 1
                continue
            disorder.append(windows.lag(edit.ts))
            if windows.add(edit):
                counts["aggregated"] += 1
            else:
                counts["late_dropped"] += 1
            out.add_windows(windows.pop_closed(), closed_by_watermark=True, coverage=(min_ts, max_ts))
    finally:
        consumer.close()

    if min_ts is not None:
        out.add_windows(windows.pop_open(), closed_by_watermark=False, coverage=(min_ts, max_ts))

    closed = {r["janela_inicio"] for r in out.rows if r["fechada"]}
    forced = {r["janela_inicio"] for r in out.rows if not r["fechada"]}
    metrics = {
        "janela_segundos": config.WINDOW_SECONDS,
        "atraso_permitido_segundos": config.ALLOWED_LATENESS_SECONDS,
        "tipos_agregados": list(config.EVENT_TYPES),
        "captura": {
            "inicio_utc": _iso(first_recv),
            "fim_utc": _iso(last_recv),
            "duracao_segundos": _round(last_recv - first_recv) if first_recv is not None else None,
            "evento_mais_antigo_utc": _iso(min_ts),
            "evento_mais_recente_utc": _iso(max_ts),
        },
        "contagens": {
            "consumidos_do_topico": counts["consumed"],
            "agregados": counts["aggregated"],
            "tardios_descartados": counts["late_dropped"],
            "ignorados_por_tipo": counts["ignored_type"],
            "invalidos": counts["invalid"],
            "janelas_fechadas_por_watermark": len(closed),
            "janelas_encerradas_no_fim_do_run": len(forced),
        },
        "latencia_segundos": {
            "ponta_a_ponta_gravacao": summary(out.write_latency["watermark"]),
            "ponta_a_ponta_gravacao_fim_do_run": summary(out.write_latency["fim_do_run"]),
            "ate_consumo": summary(out.consume_latency),
            "desordem_observada": summary(disorder),
            "histograma_gravacao": histogram(out.write_latency["watermark"]),
        },
        "relogio_local_offset_segundos": clock_offset_s,
    }
    if producer_stats is not None and producer_stats.exists():
        metrics["producer"] = json.loads(producer_stats.read_text(encoding="utf-8"))
    (out_dir / "metrics.json").write_text(json.dumps(metrics, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info("agregado gravado: %s", metrics["contagens"])
    return metrics


def _iso(epoch: float | None) -> str | None:
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat(timespec="seconds")


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run-seconds", type=float, default=310.0, help="tempo mínimo de consumo")
    ap.add_argument("--bootstrap", default=config.BOOTSTRAP_SERVERS)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--producer-stats", type=Path, default=None)
    ap.add_argument("--clock-offset-s", type=float, default=None,
                    help="offset do relógio local medido antes do run (ex.: `sntp time.apple.com`)")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run(args.run_seconds, args.bootstrap, args.out_dir, args.producer_stats, args.clock_offset_s)


if __name__ == "__main__":
    main()
