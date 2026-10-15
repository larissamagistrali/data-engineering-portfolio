"""Percentis e histograma da latência (funções puras)."""
from wikipedia_streaming.aggregator import histogram, percentile, summary


def test_percentil_por_posto():
    vals = [1.0, 2.0, 3.0, 4.0]
    assert percentile(vals, 0.5) == 2.0
    assert percentile(vals, 0.95) == 4.0
    assert percentile([], 0.5) is None


def test_summary_vazio_e_normal():
    assert summary([])["n"] == 0
    s = summary([10.0, 20.0])
    assert (s["n"], s["p50"], s["max"]) == (2, 10.0, 20.0)


def test_histograma_agrupa_e_satura_no_ultimo_bin():
    h = histogram([0.5, 14.9, 15.0, 1000.0])
    assert h["eventos"][0] == 2
    assert h["eventos"][1] == 1
    assert h["eventos"][-1] == 1
    assert len(h["faixas_s"]) == len(h["eventos"])


def test_output_marca_janelas_parciais_e_separa_latencias(tmp_path):
    import pandas as pd

    from wikipedia_streaming.aggregator import Output
    from wikipedia_streaming.windowing import ClosedWindow, Edit

    out = Output(tmp_path)
    w0 = ClosedWindow(start=0, edits=[Edit(ts=30, wiki="enwiki", bot=False, event_type="edit", recv=31)])
    w1 = ClosedWindow(start=60, edits=[Edit(ts=70, wiki="enwiki", bot=True, event_type="edit", recv=70.5)])
    out.add_windows([w0], closed_by_watermark=True, coverage=(10, 130))
    out.add_windows([w1], closed_by_watermark=False, coverage=(10, 130))
    df = pd.read_parquet(tmp_path / "agregado_por_minuto.parquet")
    assert df["janela_parcial"].tolist() == [True, False]  # [0,60) começa antes da captura (10)
    assert df["fechada"].tolist() == [True, False]
    assert len(out.write_latency["watermark"]) == 1
    assert len(out.write_latency["fim_do_run"]) == 1
    assert out.consume_latency == [1.0, 0.5]
