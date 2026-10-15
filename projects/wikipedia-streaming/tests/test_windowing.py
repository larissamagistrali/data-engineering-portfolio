"""Janelas tumbling, watermark e tardios com eventos sintéticos."""
from wikipedia_streaming.windowing import Edit, TumblingWindows, aggregate, parse_epoch, window_start


def ed(ts, wiki="enwiki", bot=False, event_type="edit"):
    return Edit(ts=ts, wiki=wiki, bot=bot, event_type=event_type)


def test_window_start_alinha_ao_minuto():
    assert window_start(0) == 0
    assert window_start(59.9) == 0
    assert window_start(60) == 60
    assert window_start(125) == 120


def test_parse_epoch_aceita_sufixo_z():
    assert parse_epoch("1970-01-01T00:01:00.500Z") == 60.5


def test_watermark_marca_tardios_e_fecha_janelas():
    w = TumblingWindows(size=60, lateness=120)
    assert w.add(ed(0))
    assert w.watermark == -120
    assert w.add(ed(130))  # max=130 -> watermark=10
    assert w.pop_closed() == []  # janela [0,60) fecha só quando seu fim <= watermark
    assert w.add(ed(250))  # watermark=130: fecham [0,60) e [60,120) (nenhum evento em [60,120))
    closed = w.pop_closed()
    assert [c.start for c in closed] == [0]
    assert len(closed[0].edits) == 1
    assert not w.add(ed(100))  # 100 < 130: tardio
    assert not w.add(ed(129.9))  # tardio no limite
    assert w.add(ed(130))  # igual ao watermark não é tardio
    assert w.late_dropped == 2
    assert w.accepted == 4


def test_pop_open_devolve_janelas_restantes():
    w = TumblingWindows()
    w.add(ed(10))
    w.add(ed(70))
    assert [c.start for c in w.pop_open()] == [0, 60]
    assert w.pop_open() == []


def test_aggregate_separa_bot_humano_tipo_e_ignora_outros_tipos():
    edits = [
        ed(1, "enwiki", False, "edit"),
        ed(2, "enwiki", True, "edit"),
        ed(3, "enwiki", False, "new"),
        ed(4, "dewiki", False, "edit"),
        ed(5, "enwiki", False, "external"),  # fora de EVENT_TYPES
    ]
    c = aggregate(edits)
    assert c[("enwiki", "humano", "edit")] == 1
    assert c[("enwiki", "bot", "edit")] == 1
    assert c[("enwiki", "humano", "new")] == 1
    assert c[("dewiki", "humano", "edit")] == 1
    assert sum(c.values()) == 4


def test_lag_mede_desordem_em_relacao_ao_maior_timestamp():
    w = TumblingWindows()
    assert w.lag(100) == 0.0  # nada visto ainda
    w.add(ed(100))
    assert w.lag(95) == 5.0
    assert w.lag(130) == 0.0  # evento à frente não está atrasado
