"""Testes do gerador sintético: determinismo, schema do Olist e coerência dos dados."""
import pandas as pd
import pytest

from olist_warehouse.make_sample import generate

EXPECTED_COLUMNS = {
    "olist_customers_dataset": ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"],
    "olist_orders_dataset": ["order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at",
                             "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"],
    "olist_order_items_dataset": ["order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value"],
    "olist_order_payments_dataset": ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"],
    "olist_order_reviews_dataset": ["review_id", "order_id", "review_score", "review_comment_title", "review_comment_message",
                                    "review_creation_date", "review_answer_timestamp"],
    "olist_products_dataset": ["product_id", "product_category_name", "product_name_lenght", "product_description_lenght",
                               "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"],
    "olist_sellers_dataset": ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
    "product_category_name_translation": ["product_category_name", "product_category_name_english"],
}


@pytest.fixture(scope="module")
def tables() -> dict[str, pd.DataFrame]:
    return generate(2500, seed=7)


def test_schema_matches_olist(tables):
    for name, cols in EXPECTED_COLUMNS.items():
        assert list(tables[name].columns) == cols, name


def test_deterministic():
    a, b = generate(300, seed=1), generate(300, seed=1)
    for name in a:
        pd.testing.assert_frame_equal(a[name], b[name])
    c = generate(300, seed=2)
    assert not a["olist_orders_dataset"].equals(c["olist_orders_dataset"])


def test_referential_integrity(tables):
    orders = set(tables["olist_orders_dataset"]["order_id"])
    assert set(tables["olist_order_items_dataset"]["order_id"]) <= orders
    assert set(tables["olist_order_payments_dataset"]["order_id"]) <= orders
    assert set(tables["olist_order_reviews_dataset"]["order_id"]) <= orders
    assert set(tables["olist_order_items_dataset"]["product_id"]) <= set(tables["olist_products_dataset"]["product_id"])
    assert set(tables["olist_order_items_dataset"]["seller_id"]) <= set(tables["olist_sellers_dataset"]["seller_id"])
    assert tables["olist_orders_dataset"]["order_id"].is_unique


def test_payments_match_items_for_orders_with_items(tables):
    items = tables["olist_order_items_dataset"]
    total = (items["price"] + items["freight_value"]).groupby(items["order_id"]).sum()
    paid = tables["olist_order_payments_dataset"].groupby("order_id")["payment_value"].sum()
    assert ((paid.reindex(total.index) - total).abs() < 0.05).all()


def test_review_score_correlates_with_delay(tables):
    o = tables["olist_orders_dataset"]
    o = o[o["order_status"] == "delivered"].dropna(subset=["order_delivered_customer_date"])
    late = pd.to_datetime(o["order_delivered_customer_date"]).dt.normalize() > pd.to_datetime(o["order_estimated_delivery_date"])
    r = tables["olist_order_reviews_dataset"].drop_duplicates("order_id").set_index("order_id")["review_score"]
    scores = o.assign(late=late.to_numpy()).set_index("order_id").join(r, how="inner")
    assert scores.loc[scores["late"], "review_score"].mean() < scores.loc[~scores["late"], "review_score"].mean() - 0.8


def test_value_ranges(tables):
    assert tables["olist_order_reviews_dataset"]["review_score"].between(1, 5).all()
    assert (tables["olist_order_items_dataset"]["price"] > 0).all()
    assert (tables["olist_order_items_dataset"]["freight_value"] >= 0).all()
    assert set(tables["olist_orders_dataset"]["order_status"]) <= {
        "delivered", "shipped", "canceled", "unavailable", "invoiced", "processing", "approved", "created"}
