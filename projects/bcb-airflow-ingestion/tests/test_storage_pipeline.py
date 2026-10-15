from datetime import date, datetime, timezone

import pandas as pd

from bcb_ingestion import pipeline
from bcb_ingestion.storage import build_gold, merge_observations, read_silver, write_silver
from bcb_ingestion.validate import validate_observations



class FakeResponse:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class DailyServer:
    """Simula a API: uma observação por dia útil (sábado/domingo ficam de fora), valor fixo por dia."""

    def __init__(self, fail_codes=()):
        self.fail_codes = set(fail_codes)
        self.urls = []

    def get(self, url, headers=None, timeout=None):
        self.urls.append(url)
        codigo = int(url.split("bcdata.sgs.")[1].split("/")[0])
        if codigo in self.fail_codes:
            return FakeResponse(400, None)
        ini = datetime.strptime(url.split("dataInicial=")[1][:10], "%d/%m/%Y").date()
        fim = datetime.strptime(url.split("dataFinal=")[1][:10], "%d/%m/%Y").date()
        rows = []
        d = ini
        while d <= fim:
            if d.weekday() < 5:
                rows.append({"data": d.strftime("%d/%m/%Y"), "valor": f"{(d.toordinal() % 100) / 10:.2f}"})
            d = date.fromordinal(d.toordinal() + 1)
        return FakeResponse(200, rows)


def test_escrita_silver_e_idempotente(tmp_path):
    from bcb_ingestion.client import parse_observations

    silver = tmp_path / "silver"
    new = parse_observations([{"data": "01/01/2024", "valor": "1.0"}, {"data": "02/01/2024", "valor": "2.0"}], 1)
    _, n1 = write_silver(1, new, silver)
    primeira = read_silver(1, silver)
    _, n2 = write_silver(1, new, silver)  # mesma carga de novo
    segunda = read_silver(1, silver)
    assert n1 == 2 and n2 == 0
    pd.testing.assert_frame_equal(primeira, segunda)


def test_merge_troca_valor_revisado_sem_duplicar():
    from bcb_ingestion.client import parse_observations

    old = parse_observations([{"data": "01/01/2024", "valor": "1.0"}], 433)
    new = parse_observations([{"data": "01/01/2024", "valor": "1.5"}, {"data": "02/01/2024", "valor": "2.0"}], 433)
    merged = merge_observations(old, new)
    assert len(merged) == 2
    assert merged.loc[merged["data"] == "2024-01-01", "valor"].iloc[0] == 1.5


def test_pipeline_incremental_fim_a_fim_e_reexecucao_sem_novidade(tmp_path, monkeypatch):
    monkeypatch.setattr("bcb_ingestion.pipeline.SERIES", {1: "dolar_venda", 432: "selic_meta"})
    hoje = date(2024, 3, 10)
    server = DailyServer()
    run1 = pipeline.run_all(tmp_path, hoje, session=server, sleep=lambda s: None)
    gold1 = pd.read_parquet(tmp_path / "series_gold.parquet")
    assert run1["series"] == 2 and run1["linhas_gold"] > 0
    assert gold1["codigo"].isin([1, 432]).all()
    assert (tmp_path / "series_status.json").exists()
    assert not validate_observations(gold1[gold1["codigo"] == 1], today=hoje)

    # Segunda execução no mesmo dia: não deve gerar linhas novas nem duplicar
    pipeline.run_all(tmp_path, hoje, session=server, sleep=lambda s: None)
    gold2 = pd.read_parquet(tmp_path / "series_gold.parquet")
    pd.testing.assert_frame_equal(gold1, gold2)

    # Dia seguinte: a coleta começa na última data (inclusive) e só acrescenta
    chamadas_antes = len(server.urls)
    pipeline.run_all(tmp_path, date(2024, 3, 12), session=server, sleep=lambda s: None)
    assert len(server.urls) > chamadas_antes
    # última data gravada é sexta 08/03/2024 (fim de semana não tem observação)
    assert all("dataInicial=08/03/2024" in u for u in server.urls[chamadas_antes:])
    gold3 = pd.read_parquet(tmp_path / "series_gold.parquet")
    assert gold3["data"].max() > gold1["data"].max()
    assert not gold3.duplicated(["codigo", "data"]).any()


def test_falha_de_uma_serie_nao_gera_gold(tmp_path, monkeypatch):
    import pytest
    import requests

    monkeypatch.setattr("bcb_ingestion.pipeline.SERIES", {1: "dolar_venda"})
    with pytest.raises(requests.HTTPError):
        pipeline.fetch_series(1, tmp_path, date(2024, 1, 5), session=DailyServer({1}), sleep=lambda s: None)


def test_build_gold_sem_silver_falha_claramente(tmp_path):
    import pytest

    with pytest.raises(FileNotFoundError):
        build_gold(tmp_path / "silver")
