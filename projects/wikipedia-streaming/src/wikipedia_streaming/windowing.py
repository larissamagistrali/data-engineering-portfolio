"""Funções puras de agregação em janelas tumbling com watermark (sem Kafka, sem rede).

Regra de atraso: o watermark é `maior timestamp de evento visto - ALLOWED_LATENESS`.
Evento com timestamp abaixo do watermark é tardio: é contado em `late_dropped` e não entra
no agregado. Uma janela só fecha (e é emitida) quando seu fim fica abaixo do watermark.
"""
from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime

from .config import ALLOWED_LATENESS_SECONDS, EVENT_TYPES, WINDOW_SECONDS


@dataclass(frozen=True)
class Edit:
    """Campos de uma mudança recente que interessam à agregação."""

    ts: float  # timestamp do evento (epoch, segundos), vindo de meta.dt
    wiki: str
    bot: bool
    event_type: str
    recv: float = 0.0  # instante (epoch) em que o agregador consumiu o evento; para medir latência


@dataclass
class ClosedWindow:
    start: int  # epoch de início da janela
    edits: list[Edit] = field(default_factory=list)


def parse_epoch(iso: str) -> float:
    """'2026-10-14T22:41:37.078Z' -> epoch em segundos (float)."""
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()


def window_start(ts: float, size: int = WINDOW_SECONDS) -> int:
    return int(ts // size) * size


def aggregate(edits: Iterable[Edit]) -> Counter:
    """Conta eventos por (wiki, tipo de editor, tipo de evento). Tipos fora de EVENT_TYPES não entram."""
    counts: Counter = Counter()
    for e in edits:
        if e.event_type in EVENT_TYPES:
            counts[(e.wiki, "bot" if e.bot else "humano", e.event_type)] += 1
    return counts


class TumblingWindows:
    """Estado da agregação: janelas abertas, watermark e contagem de tardios."""

    def __init__(self, size: int = WINDOW_SECONDS, lateness: int = ALLOWED_LATENESS_SECONDS) -> None:
        self.size = size
        self.lateness = lateness
        self.max_ts: float | None = None
        self._open: dict[int, list[Edit]] = {}
        self.late_dropped = 0
        self.accepted = 0

    @property
    def watermark(self) -> float | None:
        return None if self.max_ts is None else self.max_ts - self.lateness

    def lag(self, ts: float) -> float:
        """Quanto um evento chega atrás do maior timestamp já visto (desordem observada, em segundos)."""
        return 0.0 if self.max_ts is None else max(0.0, self.max_ts - ts)

    def add(self, edit: Edit) -> bool:
        """Adiciona um evento. Devolve False se ele é tardio (descartado)."""
        wm = self.watermark
        if wm is not None and edit.ts < wm:
            self.late_dropped += 1
            return False
        self._open.setdefault(window_start(edit.ts, self.size), []).append(edit)
        self.accepted += 1
        self.max_ts = edit.ts if self.max_ts is None else max(self.max_ts, edit.ts)
        return True

    def pop_closed(self) -> list[ClosedWindow]:
        """Remove e devolve as janelas cujo fim está abaixo do watermark, em ordem cronológica."""
        wm = self.watermark
        if wm is None:
            return []
        done = sorted(s for s in self._open if s + self.size <= wm)
        return [ClosedWindow(start=s, edits=self._open.pop(s)) for s in done]

    def pop_open(self) -> list[ClosedWindow]:
        """Devolve as janelas ainda abertas no fim do run (o watermark não avança sem novos eventos)."""
        out = [ClosedWindow(start=s, edits=v) for s, v in sorted(self._open.items())]
        self._open.clear()
        return out
