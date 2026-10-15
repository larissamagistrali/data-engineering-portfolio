"""Exporta a camada gold do dbt para artefatos pequenos que o app Streamlit lê.

Saídas em `data/`: parquet (zstd) dos marts/KPIs, `lineage.json` (nós e arestas do manifest),
`dbt_run_results.json` (resumo enxuto dos modelos/testes) e `data_dictionary.json`.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

PROJECT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_DB = PROJECT_DIR / "data" / "raw" / "olist.duckdb"
DEFAULT_TARGET = PROJECT_DIR / "dbt" / "target"
DEFAULT_OUT = PROJECT_DIR / "data"

# schema.tabela exportadas -> nome do parquet
EXPORT_TABLES = {
    "gold.kpi_overview": "kpi_overview",
    "gold.kpi_monthly_revenue": "kpi_monthly_revenue",
    "gold.kpi_delivery_sla": "kpi_delivery_sla",
    "gold.kpi_category_reviews": "kpi_category_reviews",
    "gold.kpi_top_sellers": "kpi_top_sellers",
    "gold.kpi_repurchase_cohort": "kpi_repurchase_cohort",
    "marts.fct_orders": "fct_orders",
    "marts.fct_order_items": "fct_order_items",
    "marts.dim_customers": "dim_customers",
    "marts.dim_products": "dim_products",
    "marts.dim_sellers": "dim_sellers",
    "marts.dim_date": "dim_date",
}
LAYERS = ("source", "staging", "intermediate", "marts", "gold")


def _layer(node: dict[str, Any]) -> str:
    """Camada do nó a partir do schema/pasta (source, staging, intermediate, marts, gold)."""
    if node["resource_type"] == "source":
        return "source"
    schema = node.get("schema", "")
    if schema in LAYERS:
        return schema
    return "marts"


def _test_counts(manifest: dict[str, Any]) -> dict[str, int]:
    """Quantidade de testes por nó (modelo/fonte) alvo."""
    counts: dict[str, int] = {}
    for node in manifest["nodes"].values():
        if node["resource_type"] != "test":
            continue
        for parent in node.get("depends_on", {}).get("nodes", []):
            if parent.startswith(("model.", "source.")):
                counts[parent] = counts.get(parent, 0) + 1
    return counts


def build_lineage(manifest: dict[str, Any]) -> dict[str, Any]:
    """Nós (fontes e modelos) e arestas pai -> filho derivados do manifest.json do dbt."""
    tests = _test_counts(manifest)
    nodes: list[dict[str, Any]] = []
    for uid, node in {**manifest["sources"], **manifest["nodes"]}.items():
        if node["resource_type"] not in ("source", "model"):
            continue
        name = node["name"] if node["resource_type"] == "model" else f"{node['source_name']}.{node['name']}"
        nodes.append({
            "id": uid,
            "name": name,
            "resource_type": node["resource_type"],
            "layer": _layer(node),
            "materialized": node.get("config", {}).get("materialized"),
            "description": (node.get("description") or "").strip(),
            "tests": tests.get(uid, 0),
        })
    known = {n["id"] for n in nodes}
    edges = [
        {"source": parent, "target": child}
        for child, parents in manifest["parent_map"].items()
        for parent in parents
        if child in known and parent in known
    ]
    nodes.sort(key=lambda n: (LAYERS.index(n["layer"]), n["name"]))
    return {"nodes": nodes, "edges": edges}


def summarize_run_results(run_results: dict[str, Any], manifest: dict[str, Any]) -> dict[str, Any]:
    """Resumo enxuto do `dbt build`: contagens por status e uma linha por modelo/teste."""
    items = []
    for r in run_results["results"]:
        node = manifest["nodes"].get(r["unique_id"], {})
        rtype = node.get("resource_type", "model")
        row: dict[str, Any] = {
            "unique_id": r["unique_id"],
            "name": node.get("name", r["unique_id"].split(".")[-1]),
            "resource_type": rtype,
            "status": r["status"],
            "execution_time": round(r["execution_time"], 3),
            "failures": r.get("failures"),
            "message": r.get("message"),
        }
        if rtype == "test":
            meta = node.get("test_metadata")
            row["test_kind"] = "generic" if meta else "singular"
            row["test_name"] = meta["name"] if meta else node["name"]
            row["severity"] = node.get("config", {}).get("severity", "error").lower()
            row["column"] = node.get("column_name")
            attached = node.get("attached_node") or next(iter(node.get("depends_on", {}).get("nodes", [])), None)
            row["model"] = attached.split(".")[-1] if attached else None
        else:
            row["layer"] = _layer(node) if node else None
            row["materialized"] = node.get("config", {}).get("materialized")
        items.append(row)
    summary: dict[str, int] = {}
    for it in items:
        key = f"{it['resource_type'] if it['resource_type'] == 'test' else 'model'}_{it['status']}"
        summary[key] = summary.get(key, 0) + 1
    return {
        "dbt_version": run_results.get("metadata", {}).get("dbt_version"),
        "generated_at": run_results.get("metadata", {}).get("generated_at"),
        "elapsed_seconds": round(run_results.get("elapsed_time", 0), 2),
        "summary": summary,
        "results": items,
    }


def build_dictionary(manifest: dict[str, Any], catalog: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Dicionário de dados: por modelo, descrição e colunas (tipo vem do catalog.json, se houver)."""
    types: dict[str, dict[str, str]] = {}
    for uid, node in (catalog or {}).get("nodes", {}).items():
        types[uid] = {c.lower(): v["type"] for c, v in node["columns"].items()}
    out = []
    for uid, node in manifest["nodes"].items():
        if node["resource_type"] != "model":
            continue
        cols = []
        for cname, col in node.get("columns", {}).items():
            cols.append({"column": cname, "description": col.get("description", ""),
                         "type": types.get(uid, {}).get(cname.lower())})
        out.append({
            "model": node["name"], "layer": _layer(node), "description": (node.get("description") or "").strip(),
            "documented_columns": cols,
            "all_columns": [{"column": c, "type": t} for c, t in types.get(uid, {}).items()],
        })
    out.sort(key=lambda m: (LAYERS.index(m["layer"]), m["model"]))
    return out


def export_tables(db_path: Path, out_dir: Path, tables: dict[str, str] | None = None) -> dict[str, int]:
    """Copia as tabelas gold/marts do DuckDB para parquet zstd e devolve {arquivo: linhas}."""
    out_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path), read_only=True)
    counts = {}
    try:
        for table, name in (tables or EXPORT_TABLES).items():
            dest = str(out_dir / f"{name}.parquet").replace("'", "''")
            con.execute(f"copy (select * from {table}) to '{dest}' (format parquet, compression zstd, compression_level 9)")
            counts[name] = con.execute(f"select count(*) from {table}").fetchone()[0]
    finally:
        con.close()
    return counts


def export_all(db_path: Path = DEFAULT_DB, target_dir: Path = DEFAULT_TARGET, out_dir: Path = DEFAULT_OUT) -> dict[str, Any]:
    manifest = json.loads((target_dir / "manifest.json").read_text())
    run_results = json.loads((target_dir / "run_results.json").read_text())
    catalog_path = target_dir / "catalog.json"
    catalog = json.loads(catalog_path.read_text()) if catalog_path.exists() else None
    counts = export_tables(db_path, out_dir)
    dumps = {
        "lineage.json": build_lineage(manifest),
        "dbt_run_results.json": summarize_run_results(run_results, manifest),
        "data_dictionary.json": build_dictionary(manifest, catalog),
    }
    for fname, payload in dumps.items():
        (out_dir / fname).write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    meta = {"exported_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "rows": counts}
    (out_dir / "export_meta.json").write_text(json.dumps(meta, indent=1), encoding="utf-8")
    return meta


def main() -> None:
    print(json.dumps(export_all(), indent=1))


if __name__ == "__main__":
    main()
