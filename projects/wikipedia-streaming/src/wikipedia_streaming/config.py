"""Configuração padrão (sobrescrevível por variável de ambiente ou argumento de linha de comando)."""
from __future__ import annotations

import os

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
# Identificação exigida pela política de uso da Wikimedia (User-Agent descritivo).
USER_AGENT = "portfolio-demo/0.1 (https://github.com/larissamagistrali)"

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("WIKI_TOPIC", "wiki-recentchange")
CONSUMER_GROUP = os.getenv("WIKI_GROUP", "wiki-aggregator")

WINDOW_SECONDS = 60
ALLOWED_LATENESS_SECONDS = 120
# Tipos de evento agregados; os demais (ex.: "external") são contados como ignorados.
EVENT_TYPES = ("edit", "new", "log", "categorize")

BACKOFF_BASE_SECONDS = 1.0
BACKOFF_MAX_SECONDS = 60.0
