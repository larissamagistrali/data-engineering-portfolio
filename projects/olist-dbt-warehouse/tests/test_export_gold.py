"""Testes das funções puras de exportação (linhagem, resumo de testes, dicionário) e do load_raw."""
import duckdb

from olist_warehouse import export_gold, load_raw, make_sample

MANIFEST = {
    "sources": {"source.p.olist.orders": {"resource_type": "source", "source_name": "olist", "name": "orders",
                                           "schema": "raw", "description": "raw"}},
    "nodes": {
        "model.p.stg_orders": {"resource_type": "model", "name": "stg_orders", "schema": "staging",
                               "description": "stg", "config": {"materialized": "view"},
                               "columns": {"order_id": {"description": "chave"}}},
        "model.p.fct_orders": {"resource_type": "model", "name": "fct_orders", "schema": "marts",
                               "description": "fato", "config": {"materialized": "table"}, "columns": {}},
        "test.p.unique_stg_orders_order_id.x": {
            "resource_type": "test", "name": "unique_stg_orders_order_id", "depends_on": {"nodes": ["model.p.stg_orders"]},
            "test_metadata": {"name": "unique"}, "column_name": "order_id", "attached_node": "model.p.stg_orders",
            "config": {"severity": "ERROR"}},
        "test.p.assert_x": {"resource_type": "test", "name": "assert_x", "depends_on": {"nodes": ["model.p.fct_orders"]},
                            "config": {"severity": "warn"}},
    },
    "parent_map": {
        "source.p.olist.orders": [],
        "model.p.stg_orders": ["source.p.olist.orders"],
        "model.p.fct_orders": ["model.p.stg_orders"],
        "test.p.assert_x": ["model.p.fct_orders"],
    },
}
RUN_RESULTS = {
    "metadata": {"dbt_version": "9.9", "generated_at": "2026-01-01T00:00:00Z"},
    "elapsed_time": 1.234,
    "results": [
        {"unique_id": "model.p.stg_orders", "status": "success", "execution_time": 0.1, "failures": None, "message": "OK"},
        {"unique_id": "test.p.unique_stg_orders_order_id.x", "status": "pass", "execution_time": 0.01, "failures": 0, "message": None},
        {"unique_id": "test.p.assert_x", "status": "warn", "execution_time": 0.02, "failures": 3, "message": "Got 3"},
    ],
}


def test_build_lineage_nodes_and_edges():
    lin = export_gold.build_lineage(MANIFEST)
    layers = [n["layer"] for n in lin["nodes"]]
    assert layers == ["source", "staging", "marts"]
    assert {"source": "model.p.stg_orders", "target": "model.p.fct_orders"} in lin["edges"]
    assert all(e["source"] != e["target"] for e in lin["edges"])
    assert len(lin["edges"]) == 2  # testes não entram no grafo
    assert {n["name"]: n["tests"] for n in lin["nodes"]}["stg_orders"] == 1


def test_summarize_run_results():
    res = export_gold.summarize_run_results(RUN_RESULTS, MANIFEST)
    assert res["summary"] == {"model_success": 1, "test_pass": 1, "test_warn": 1}
    by_name = {r["name"]: r for r in res["results"]}
    assert by_name["unique_stg_orders_order_id"]["test_kind"] == "generic"
    assert by_name["unique_stg_orders_order_id"]["severity"] == "error"
    assert by_name["assert_x"]["test_kind"] == "singular" and by_name["assert_x"]["failures"] == 3


def test_build_dictionary_uses_catalog_types():
    catalog = {"nodes": {"model.p.stg_orders": {"columns": {"ORDER_ID": {"type": "VARCHAR"}}}}}
    d = export_gold.build_dictionary(MANIFEST, catalog)
    stg = next(m for m in d if m["model"] == "stg_orders")
    assert stg["documented_columns"][0] == {"column": "order_id", "description": "chave", "type": "VARCHAR"}


def test_load_raw_and_export_tables(tmp_path):
    raw = tmp_path / "raw"
    make_sample.write_csvs(make_sample.generate(200, seed=3), raw)
    counts = load_raw.load_raw(raw, tmp_path / "w.duckdb")
    assert counts["orders"] == 200
    con = duckdb.connect(str(tmp_path / "w.duckdb"))
    assert con.execute("select typeof(order_purchase_timestamp) from raw.orders limit 1").fetchone()[0] == "VARCHAR"
    con.execute("create schema gold")
    con.execute("create table gold.kpi_x as select 1 as x union all select 2")
    con.close()
    out = tmp_path / "out"
    assert export_gold.export_tables(tmp_path / "w.duckdb", out, {"gold.kpi_x": "kpi_x"}) == {"kpi_x": 2}
    assert duckdb.sql(f"select count(*) from read_parquet('{out / 'kpi_x.parquet'}')").fetchone()[0] == 2
