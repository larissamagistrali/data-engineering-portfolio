"""Parser SSE com linhas sintéticas (sem rede)."""
from wikipedia_streaming.sse import parse_sse


def test_comentarios_e_eventos_basicos():
    lines = [":ok", "", "event: message", 'id: [{"a":1}]', 'data: {"x":1}', ""]
    events = list(parse_sse(lines))
    assert len(events) == 1
    assert events[0].event == "message"
    assert events[0].data == '{"x":1}'
    assert events[0].id == '[{"a":1}]'


def test_data_multilinha_e_id_persiste_entre_eventos():
    lines = ["id: 42", "data: linha1", "data: linha2", "", "data: outro", ""]
    events = list(parse_sse(lines))
    assert [e.data for e in events] == ["linha1\nlinha2", "outro"]
    assert [e.id for e in events] == ["42", "42"]  # id vale até ser trocado
    assert [e.event for e in events] == ["message", "message"]


def test_retry_e_campos_desconhecidos_sao_ignorados():
    lines = ["retry: 3000", "foo: bar", "data: ok", ""]
    events = list(parse_sse(lines))
    assert [e.data for e in events] == ["ok"]


def test_evento_incompleto_no_fim_e_descartado():
    lines = ["data: completo", "", "data: sem fim de evento"]
    events = list(parse_sse(lines))
    assert [e.data for e in events] == ["completo"]


def test_crlf_e_valor_sem_espaco_inicial():
    lines = ["data:sem-espaco\r\n", "\r\n"]
    events = list(parse_sse(lines))
    assert [e.data for e in events] == ["sem-espaco"]
