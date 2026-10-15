"""Parser mínimo de Server-Sent Events (https://html.spec.whatwg.org/#event-stream-interpretation).

Puro: recebe linhas de texto e devolve eventos, sem rede, para ser testável com entradas sintéticas.
"""
from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass


@dataclass(frozen=True)
class SSEEvent:
    event: str  # tipo do evento ("message" quando ausente, conforme a especificação)
    data: str  # linhas de `data:` unidas por "\n"
    id: str | None  # último `id:` visto no fluxo (persiste entre eventos)


def parse_sse(lines: Iterable[str]) -> Iterator[SSEEvent]:
    """Converte linhas de um fluxo SSE em eventos completos.

    Um evento só é emitido ao encontrar uma linha vazia; um evento incompleto no fim do fluxo
    é descartado (como manda a especificação). Linhas que começam com ':' são comentários
    (keep-alive) e são ignoradas.
    """
    event_type = "message"
    data_lines: list[str] = []
    last_id: str | None = None
    for raw in lines:
        line = raw.rstrip("\r\n")
        if line == "":
            if data_lines:
                yield SSEEvent(event=event_type, data="\n".join(data_lines), id=last_id)
            event_type = "message"
            data_lines = []
            continue
        if line.startswith(":"):
            continue
        field, _, value = line.partition(":")
        if value.startswith(" "):
            value = value[1:]
        if field == "event":
            event_type = value or "message"
        elif field == "data":
            data_lines.append(value)
        elif field == "id":
            if "\0" not in value:
                last_id = value
        # "retry" e campos desconhecidos são ignorados
