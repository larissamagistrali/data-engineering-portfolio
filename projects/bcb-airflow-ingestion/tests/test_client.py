from datetime import date

import pytest
import requests

from bcb_ingestion.client import USER_AGENT, build_url, fetch_json, parse_observations, windows


class FakeResponse:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class FakeSession:
    """Devolve respostas roteirizadas e guarda as chamadas feitas."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, headers=None, timeout=None):
        self.calls.append({"url": url, "headers": headers, "timeout": timeout})
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def test_build_url_usa_formato_dd_mm_aaaa():
    url = build_url(432, date(2024, 1, 1), date(2024, 12, 31))
    assert url == ("https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados"
                   "?formato=json&dataInicial=01/01/2024&dataFinal=31/12/2024")


def test_windows_respeita_limite_e_cobre_intervalo():
    jan = windows(date(2010, 1, 1), date(2026, 10, 14))
    assert jan[0][0] == date(2010, 1, 1) and jan[-1][1] == date(2026, 10, 14)
    for (a, b), (c, _) in zip(jan, jan[1:]):
        assert (c - b).days == 1  # sem lacunas nem sobreposição
    assert all((b - a).days < 365 * 5 + 2 for a, b in jan)


def test_parse_converte_tipos_e_virgula_nao_quebra():
    payload = [{"data": "01/01/2024", "valor": "11.75"}, {"data": "02/01/2024", "valor": "11.75"}]
    df = parse_observations(payload, 432)
    assert list(df.columns) == ["data", "serie", "codigo", "valor"]
    assert df["valor"].dtype == "float64" and df["valor"].tolist() == [11.75, 11.75]
    assert df["serie"].iloc[0] == "selic_meta"
    assert str(df["data"].dtype).startswith("datetime64")


def test_parse_valor_invalido_vira_nan_para_a_validacao_pegar():
    df = parse_observations([{"data": "01/01/2024", "valor": ""}], 1)
    assert df["valor"].isna().all()


def test_parse_payload_vazio():
    assert parse_observations([], 433).empty


def test_fetch_envia_user_agent_e_devolve_json():
    session = FakeSession([FakeResponse(200, [{"data": "01/01/2024", "valor": "1"}])])
    assert fetch_json(session, "http://x", sleep=lambda s: None)[0]["valor"] == "1"
    assert session.calls[0]["headers"]["User-Agent"] == USER_AGENT
    assert USER_AGENT.startswith("portfolio-demo/")


def test_fetch_repete_em_503_com_backoff_exponencial():
    esperas = []
    session = FakeSession([FakeResponse(503), FakeResponse(429), FakeResponse(200, [])])
    assert fetch_json(session, "http://x", backoff=2.0, sleep=esperas.append) == []
    assert len(session.calls) == 3
    assert esperas == [2.0, 4.0]


def test_fetch_repete_em_erro_de_rede():
    esperas = []
    session = FakeSession([requests.ConnectionError("boom"), FakeResponse(200, [])])
    assert fetch_json(session, "http://x", sleep=esperas.append) == []
    assert len(esperas) == 1


def test_fetch_desiste_apos_retries_esgotados():
    session = FakeSession([FakeResponse(500)] * 4)
    with pytest.raises(requests.HTTPError):
        fetch_json(session, "http://x", retries=3, sleep=lambda s: None)
    assert len(session.calls) == 4


def test_fetch_nao_repete_erro_definitivo_400():
    session = FakeSession([FakeResponse(400), FakeResponse(200, [])])
    with pytest.raises(requests.HTTPError):
        fetch_json(session, "http://x", sleep=lambda s: None)
    assert len(session.calls) == 1
