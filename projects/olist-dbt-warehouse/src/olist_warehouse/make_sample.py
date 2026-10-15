"""Gerador determinístico de dados SINTÉTICOS com o schema real do Olist.

O dataset original (Brazilian E-Commerce Public Dataset by Olist, CC BY-NC-SA 4.0) só é
distribuído no Kaggle, que exige credencial. Este gerador reproduz os NOMES de tabelas/colunas
(inclusive o typo `product_name_lenght`) e distribuições plausíveis, mas **nenhum valor é real**.
Os CSVs usam os mesmos nomes de arquivo do Kaggle, então o dado real é um "drop-in".

Uso: ``python -m olist_warehouse.make_sample --orders 20000 --out data/raw``
"""
from __future__ import annotations

import argparse
import hashlib
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_DIR = Path(__file__).resolve().parents[2]
SEED = 42
START = datetime(2017, 1, 1)
END = datetime(2018, 8, 31)

# categoria PT -> (tradução EN, peso de vendas, multiplicador de preço, multiplicador de peso)
CATEGORIES: dict[str, tuple[str, float, float, float]] = {
    "cama_mesa_banho": ("bed_bath_table", 9.7, 0.9, 1.5),
    "beleza_saude": ("health_beauty", 8.8, 1.1, 0.5),
    "esporte_lazer": ("sports_leisure", 7.7, 1.0, 1.2),
    "informatica_acessorios": ("computers_accessories", 6.9, 1.2, 0.6),
    "moveis_decoracao": ("furniture_decor", 6.3, 1.0, 2.0),
    "utilidades_domesticas": ("housewares", 5.6, 0.9, 1.4),
    "relogios_presentes": ("watches_gifts", 5.1, 2.2, 0.3),
    "telefonia": ("telephony", 4.0, 0.6, 0.2),
    "automotivo": ("auto", 3.8, 1.1, 1.0),
    "brinquedos": ("toys", 3.7, 0.9, 0.9),
    "cool_stuff": ("cool_stuff", 3.4, 1.4, 0.8),
    "ferramentas_jardim": ("garden_tools", 3.3, 1.1, 2.5),
    "perfumaria": ("perfumery", 3.0, 0.9, 0.3),
    "bebes": ("baby", 2.8, 0.9, 1.0),
    "eletronicos": ("electronics", 2.4, 0.8, 0.5),
    "papelaria": ("stationery", 2.3, 0.7, 0.4),
    "fashion_bolsas_e_acessorios": ("fashion_bags_accessories", 2.0, 0.9, 0.4),
    "pet_shop": ("pet_shop", 1.7, 0.9, 1.5),
    "moveis_escritorio": ("office_furniture", 1.5, 2.6, 12.0),
    "consoles_games": ("consoles_games", 1.2, 1.5, 0.6),
    "malas_acessorios": ("luggage_accessories", 1.1, 1.3, 1.5),
    "construcao_ferramentas_construcao": ("construction_tools_construction", 0.9, 1.3, 3.0),
    "eletrodomesticos": ("home_appliances", 0.8, 1.5, 3.0),
    "livros_interesse_geral": ("books_general_interest", 0.6, 0.6, 0.4),
    "alimentos": ("food", 0.5, 0.7, 0.8),
    "bebidas": ("drinks", 0.4, 0.8, 1.8),
    "instrumentos_musicais": ("musical_instruments", 0.5, 1.8, 2.0),
    "pcs": ("computers", 0.2, 9.0, 4.0),
    "audio": ("audio", 0.35, 1.0, 0.4),
    "climatizacao": ("air_conditioning", 0.27, 1.6, 4.0),
    "agro_industria_e_comercio": ("agro_industry_and_commerce", 0.2, 3.0, 6.0),
    "artes": ("art", 0.2, 1.0, 0.8),
    "artigos_de_festas": ("party_supplies", 0.04, 0.7, 0.5),
    "artigos_de_natal": ("christmas_supplies", 0.14, 0.8, 0.5),
    "casa_conforto": ("home_comfort", 0.4, 1.0, 1.0),
    "casa_construcao": ("home_construction", 0.25, 1.3, 3.0),
    "cine_foto": ("cine_photo", 0.07, 1.2, 0.5),
    "construcao_ferramentas_ferramentas": ("construction_tools_tools", 0.04, 1.2, 3.0),
    "construcao_ferramentas_iluminacao": ("construction_tools_lights", 0.25, 1.1, 1.5),
    "construcao_ferramentas_jardim": ("costruction_tools_garden", 0.2, 1.1, 3.0),
    "construcao_ferramentas_seguranca": ("construction_tools_safety", 0.18, 1.3, 1.0),
    "dvds_blu_ray": ("dvds_blu_ray", 0.06, 0.6, 0.2),
    "eletrodomesticos_2": ("home_appliances_2", 0.25, 1.4, 3.0),
    "eletroportateis": ("small_appliances", 0.7, 1.2, 1.5),
    "fashion_calcados": ("fashion_shoes", 0.25, 0.9, 0.8),
    "fashion_esporte": ("fashion_sport", 0.03, 0.7, 0.4),
    "fashion_roupa_feminina": ("fashio_female_clothing", 0.05, 0.9, 0.3),
    "fashion_roupa_masculina": ("fashion_male_clothing", 0.1, 0.8, 0.3),
    "fashion_underwear_e_moda_praia": ("fashion_underwear_beach", 0.1, 0.6, 0.2),
    "flores": ("flowers", 0.03, 0.8, 1.0),
    "fraldas_higiene": ("diapers_and_hygiene", 0.03, 0.6, 1.5),
    "la_cuisine": ("la_cuisine", 0.01, 1.2, 1.0),
    "livros_importados": ("books_imported", 0.06, 0.9, 0.4),
    "livros_tecnicos": ("books_technical", 0.2, 0.8, 0.6),
    "market_place": ("market_place", 0.28, 0.9, 1.0),
    "moveis_cozinha_area_de_servico_jantar_e_jardim": ("kitchen_dining_laundry_garden_furniture", 0.25, 1.6, 8.0),
    "moveis_quarto": ("bed_furniture", 0.04, 1.5, 10.0),
    "moveis_sala": ("living_room_furniture", 0.13, 1.5, 8.0),
    "moveis_colchao_e_estofado": ("furniture_mattress_and_upholstery", 0.02, 2.0, 12.0),
    "musica": ("music", 0.03, 0.8, 0.3),
    "portateis_casa_forno_e_cafe": ("small_appliances_home_oven_and_coffee", 0.07, 2.0, 3.0),
    "portateis_cozinha_e_preparadores_de_alimentos": ("portable_kitchen_food_preparers", 0.02, 1.0, 2.0),
    "sinalizacao_e_seguranca": ("signaling_and_security", 0.2, 1.0, 1.0),
    "tablets_impressao_imagem": ("tablets_printing_image", 0.07, 0.9, 0.5),
    "telefonia_fixa": ("fixed_telephony", 0.23, 1.0, 0.8),
    "alimentos_bebidas": ("food_drink", 0.25, 0.7, 1.0),
    "artes_e_artesanato": ("arts_and_craftmanship", 0.02, 0.6, 0.5),
    "casa_conforto_2": ("home_comfort_2", 0.03, 1.0, 1.0),
    "cds_dvds_musicais": ("cds_dvds_musicals", 0.01, 0.8, 0.2),
    "fashion_roupa_infanto_juvenil": ("fashion_childrens_clothes", 0.01, 0.7, 0.3),
    "pc_gamer": ("pc_gamer", 0.01, 2.5, 3.0),
    "seguros_e_servicos": ("security_and_services", 0.002, 1.0, 0.1),
}

# estado -> (peso de clientes, peso de vendedores, faixa de CEP, cidades)
STATES: dict[str, tuple[float, float, tuple[int, int], list[str]]] = {
    "SP": (42.0, 60.0, (1000, 19999), ["sao paulo", "campinas", "guarulhos", "santo andre", "sao bernardo do campo", "ribeirao preto", "osasco", "sorocaba"]),
    "RJ": (12.9, 6.0, (20000, 28999), ["rio de janeiro", "niteroi", "duque de caxias", "nova iguacu", "petropolis"]),
    "MG": (11.7, 8.0, (30000, 39999), ["belo horizonte", "uberlandia", "contagem", "juiz de fora", "betim"]),
    "RS": (5.5, 3.0, (90000, 99999), ["porto alegre", "caxias do sul", "canoas", "pelotas"]),
    "PR": (5.1, 11.0, (80000, 87999), ["curitiba", "londrina", "maringa", "ponta grossa"]),
    "SC": (3.7, 7.0, (88000, 89999), ["florianopolis", "joinville", "blumenau", "itajai"]),
    "BA": (3.4, 1.0, (40000, 48999), ["salvador", "feira de santana", "vitoria da conquista"]),
    "DF": (2.1, 0.8, (70000, 73699), ["brasilia"]),
    "ES": (2.0, 0.6, (29000, 29999), ["vitoria", "vila velha", "serra"]),
    "GO": (2.0, 0.6, (72800, 76799), ["goiania", "aparecida de goiania", "anapolis"]),
    "PE": (1.6, 0.4, (50000, 56999), ["recife", "olinda", "jaboatao dos guararapes"]),
    "CE": (1.3, 0.3, (60000, 63999), ["fortaleza", "caucaia"]),
    "PA": (1.0, 0.1, (66000, 68899), ["belem", "ananindeua"]),
    "MT": (0.9, 0.2, (78000, 78899), ["cuiaba", "varzea grande"]),
    "MA": (0.7, 0.1, (65000, 65999), ["sao luis"]),
    "MS": (0.7, 0.2, (79000, 79999), ["campo grande", "dourados"]),
    "PB": (0.5, 0.1, (58000, 58999), ["joao pessoa", "campina grande"]),
    "PI": (0.5, 0.05, (64000, 64999), ["teresina"]),
    "RN": (0.5, 0.1, (59000, 59999), ["natal"]),
    "AL": (0.4, 0.05, (57000, 57999), ["maceio"]),
    "SE": (0.3, 0.05, (49000, 49999), ["aracaju"]),
    "TO": (0.3, 0.03, (77000, 77999), ["palmas"]),
    "RO": (0.25, 0.03, (76800, 76999), ["porto velho"]),
    "AM": (0.15, 0.03, (69000, 69299), ["manaus"]),
    "AC": (0.08, 0.01, (69900, 69999), ["rio branco"]),
    "AP": (0.07, 0.01, (68900, 68999), ["macapa"]),
    "RR": (0.05, 0.01, (69300, 69399), ["boa vista"]),
}
# distância logística aproximada a partir do Sudeste (1 = perto, 3 = longe)
REGION_DISTANCE = {
    **{s: 1.0 for s in ("SP", "RJ", "MG", "ES")},
    **{s: 1.5 for s in ("PR", "SC", "RS", "DF", "GO", "MS")},
    **{s: 2.2 for s in ("BA", "SE", "AL", "PE", "PB", "RN", "CE", "PI", "MT", "TO")},
    **{s: 3.0 for s in ("MA", "PA", "AP", "AM", "RR", "RO", "AC")},
}

POSITIVE = ["Recomendo", "Muito bom", "Chegou antes do prazo", "Produto de qualidade", "Gostei muito, entrega rápida",
            "Excelente vendedor", "Tudo certo com a compra", "Superou as expectativas"]
NEGATIVE = ["Produto não chegou", "Entrega atrasou muito", "Veio com defeito", "Não recomendo",
            "Demorou demais para chegar", "Produto diferente do anunciado", "Péssimo atendimento", "Faltou uma peça"]
NEUTRAL = ["Ok", "Produto razoável", "Chegou, mas a embalagem estava amassada", "Cumpre o que promete"]


def _hid(kind: str, i: int) -> str:
    """Identificador hexadecimal de 32 caracteres, determinístico (formato dos ids do Olist)."""
    return hashlib.md5(f"{kind}-{i}-{SEED}".encode()).hexdigest()


def _norm(w) -> np.ndarray:
    w = np.asarray(w, dtype=float)
    return w / w.sum()


def _fmt(ts: pd.Series) -> pd.Series:
    return ts.dt.strftime("%Y-%m-%d %H:%M:%S")


def _daily_weights() -> tuple[pd.DatetimeIndex, np.ndarray]:
    days = pd.date_range(START, END, freq="D")
    t = np.arange(len(days))
    trend = 1.0 + 0.10 * t / 30.0 * 1.6  # crescimento do marketplace
    dow = np.array([1.12, 1.10, 1.02, 0.98, 0.95, 0.78, 0.85])[days.dayofweek]
    season = np.where(days.month == 11, 1.25, 1.0) * np.where(days.month.isin([1, 12]), 1.05, 1.0)
    w = trend * dow * season
    w = np.where(days == pd.Timestamp("2017-11-24"), w * 3.2, w)  # Black Friday
    w = np.where(days == pd.Timestamp("2017-11-25"), w * 1.6, w)
    return days, _norm(w)


HOUR_W = _norm([0.9, 0.5, 0.25, 0.15, 0.1, 0.12, 0.3, 0.7, 1.2, 1.7, 2.0, 2.0, 1.7, 1.7, 1.9, 2.0, 2.0, 1.8, 1.6, 1.6, 1.8, 2.1, 2.1, 1.5])


def make_people(rng: np.random.Generator, n_orders: int, n_sellers: int):
    """Clientes (uma linha por pedido, como no Olist) e vendedores."""
    states = list(STATES)
    c_state_w = _norm([STATES[s][0] for s in states])
    s_state_w = _norm([STATES[s][1] for s in states])

    def location(state: str) -> tuple[str, int]:
        lo, hi = STATES[state][2]
        return str(rng.choice(STATES[state][3])), int(rng.integers(lo, hi + 1))

    # clientes únicos: ~4,5% dos pedidos vêm de clientes recorrentes
    u_states = rng.choice(states, size=n_orders, p=c_state_w)  # no máximo 1 cliente único por pedido
    u_loc = [location(s) for s in u_states]

    sellers = []
    s_states = rng.choice(states, size=n_sellers, p=s_state_w)
    for i, st in enumerate(s_states):
        city, zip_ = location(st)
        sellers.append({"seller_id": _hid("seller", i), "seller_zip_code_prefix": f"{zip_:05d}",
                        "seller_city": city, "seller_state": st})
    return u_states, u_loc, pd.DataFrame(sellers)


def generate(n_orders: int = 20000, seed: int = SEED) -> dict[str, pd.DataFrame]:
    rng = np.random.default_rng(seed)
    n_sellers = max(150, int(n_orders * 0.045))

    # --- pedidos: datas de compra -------------------------------------------------
    days, dw = _daily_weights()
    day_idx = rng.choice(len(days), size=n_orders, p=dw)
    hours = rng.choice(24, size=n_orders, p=HOUR_W)
    purchase = (days[day_idx] + pd.to_timedelta(hours, unit="h")
                + pd.to_timedelta(rng.integers(0, 3600, n_orders), unit="s"))
    purchase = pd.Series(purchase).sort_values().reset_index(drop=True)

    # --- clientes (customer_id por pedido; customer_unique_id reaparece em ~4,5%) ---
    u_states, u_loc, sellers = make_people(rng, n_orders, n_sellers)
    unique_of_order = np.empty(n_orders, dtype=int)
    n_unique = 0
    repeat = rng.random(n_orders) < 0.045
    for i in range(n_orders):
        if repeat[i] and n_unique > 20:
            unique_of_order[i] = rng.integers(0, n_unique)
        else:
            unique_of_order[i] = n_unique
            n_unique += 1
    customers = pd.DataFrame({
        "customer_id": [_hid("customer", i) for i in range(n_orders)],
        "customer_unique_id": [_hid("unique", u) for u in unique_of_order],
        "customer_zip_code_prefix": [f"{u_loc[u][1]:05d}" for u in unique_of_order],
        "customer_city": [u_loc[u][0] for u in unique_of_order],
        "customer_state": [u_states[u] for u in unique_of_order],
    })
    order_ids = [_hid("order", i) for i in range(n_orders)]
    c_state = customers["customer_state"].to_numpy()

    # --- categorias, produtos ---------------------------------------------------------
    cat_names = list(CATEGORIES)
    cat_w = _norm([CATEGORIES[c][1] for c in cat_names])
    n_products = int(n_orders * 0.36)
    p_cat = rng.choice(len(cat_names), size=n_products, p=cat_w)
    price_mult = np.array([CATEGORIES[c][2] for c in cat_names])[p_cat]
    weight_mult = np.array([CATEGORIES[c][3] for c in cat_names])[p_cat]
    base_price = np.clip(np.exp(rng.normal(np.log(58.0), 0.85, n_products)) * price_mult, 4.9, 6500).round(2)
    weight = np.clip(np.exp(rng.normal(np.log(450), 0.9, n_products)) * weight_mult, 20, 40000).round()
    vol = np.exp(rng.normal(np.log(8000), 0.8, n_products)) * np.sqrt(weight_mult)
    length = np.clip(np.cbrt(vol) * rng.uniform(1.0, 1.6, n_products), 7, 105).round()
    height = np.clip(np.cbrt(vol) * rng.uniform(0.4, 1.0, n_products), 2, 105).round()
    width = np.clip(np.cbrt(vol) * rng.uniform(0.7, 1.3, n_products), 6, 105).round()
    name_len = np.clip(rng.normal(48, 10, n_products), 5, 76).round()
    desc_len = np.clip(np.exp(rng.normal(6.3, 0.85, n_products)), 4, 3992).round()
    photos = rng.choice([1, 2, 3, 4, 5, 6, 7, 8], size=n_products, p=_norm([45, 22, 13, 8, 5, 3, 2, 2]))
    products = pd.DataFrame({
        "product_id": [_hid("product", i) for i in range(n_products)],
        "product_category_name": [cat_names[i] for i in p_cat],
        "product_name_lenght": name_len, "product_description_lenght": desc_len,
        "product_photos_qty": photos, "product_weight_g": weight,
        "product_length_cm": length, "product_height_cm": height, "product_width_cm": width,
    })
    # ~1,8% dos produtos sem categoria/descrição (como no dado real)
    missing = rng.random(n_products) < 0.018
    products.loc[missing, ["product_category_name", "product_name_lenght", "product_description_lenght",
                           "product_photos_qty"]] = np.nan
    translation = pd.DataFrame({
        "product_category_name": cat_names,
        "product_category_name_english": [CATEGORIES[c][0] for c in cat_names],
    })

    # cada produto pertence a um vendedor (vendedores com mais produtos vendem mais; cauda longa)
    seller_w = _norm(np.arange(1, n_sellers + 1) ** -0.65)
    seller_w = seller_w[rng.permutation(n_sellers)]
    p_seller = rng.choice(n_sellers, size=n_products, p=seller_w)
    s_state = sellers["seller_state"].to_numpy()
    product_pop = _norm(np.exp(rng.normal(0, 1.1, n_products)))

    # --- itens ------------------------------------------------------------------------
    n_items_per_order = rng.choice([1, 2, 3, 4, 5, 6], size=n_orders, p=_norm([88, 9, 1.8, 0.7, 0.3, 0.2]))
    status = rng.choice(
        ["delivered", "shipped", "canceled", "unavailable", "invoiced", "processing", "approved", "created"],
        size=n_orders, p=_norm([96.9, 1.1, 0.6, 0.6, 0.3, 0.3, 0.02, 0.01]))
    no_items = (status == "unavailable") & (rng.random(n_orders) < 0.85)
    no_items |= (status == "canceled") & (rng.random(n_orders) < 0.05)

    item_rows = []
    for i in range(n_orders):
        if no_items[i]:
            continue
        first = int(rng.choice(n_products, p=product_pop))
        picks = [first]
        for _ in range(n_items_per_order[i] - 1):
            picks.append(first if rng.random() < 0.5 else int(rng.choice(n_products, p=product_pop)))
        for k, pid in enumerate(picks, start=1):
            item_rows.append((i, k, pid))
    it = pd.DataFrame(item_rows, columns=["o", "order_item_id", "p"])
    it["s"] = p_seller[it["p"]]
    dist = np.array([REGION_DISTANCE[c_state[o]] for o in it["o"]])
    dist = np.where(c_state[it["o"]] == s_state[it["s"]], np.minimum(dist, 0.8), dist)
    price = (base_price[it["p"]] * rng.uniform(0.92, 1.08, len(it))).round(2)
    freight = (7.5 + 0.0016 * weight[it["p"]] * (0.6 + dist) + 5.5 * dist) * rng.uniform(0.85, 1.25, len(it))
    freight = np.where(rng.random(len(it)) < 0.01, 0.0, freight).round(2)

    # --- pagamentos -------------------------------------------------------------------
    totals = it.assign(price=price, freight=freight).groupby("o")[["price", "freight"]].sum().sum(axis=1)
    order_total = np.round(totals.reindex(range(n_orders)).to_numpy(), 2)
    nan_total = np.isnan(order_total)
    order_total[nan_total] = np.round(np.exp(rng.normal(4.4, 0.7, nan_total.sum())), 2)
    pay_type = rng.choice(["credit_card", "boleto", "voucher", "debit_card"], size=n_orders, p=_norm([73.5, 19.3, 5.6, 1.5]))
    multi = rng.random(n_orders) < 0.03
    inst_choices = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    inst_w = _norm([50, 12, 10, 7, 5, 4, 2, 4, 1, 5])
    pay_rows = []
    for i in range(n_orders):
        total = float(order_total[i])
        t = pay_type[i]
        if multi[i] and total > 20:
            v = round(total * float(rng.uniform(0.1, 0.6)), 2)
            pay_rows.append((i, 1, "voucher", 1, v))
            t2 = "credit_card" if rng.random() < 0.8 else "boleto"
            inst = int(rng.choice(inst_choices, p=inst_w)) if t2 == "credit_card" else 1
            pay_rows.append((i, 2, t2, inst, round(total - v, 2)))
        else:
            inst = int(rng.choice(inst_choices, p=inst_w)) if t == "credit_card" else 1
            pay_rows.append((i, 1, t, inst, total))
    pay = pd.DataFrame(pay_rows, columns=["o", "payment_sequential", "payment_type", "payment_installments", "payment_value"])
    main_type = pay.sort_values(["o", "payment_value"], ascending=[True, False]).drop_duplicates("o").set_index("o")["payment_type"]
    boleto = (main_type.reindex(range(n_orders)).to_numpy() == "boleto")

    # --- timestamps de pedido ------------------------------------------------------------
    order_dist = (pd.Series(it["o"]).to_frame("o").assign(d=dist).groupby("o")["d"].max()
                  .reindex(range(n_orders)).fillna(1.5).to_numpy())
    approve_h = np.where(boleto, rng.uniform(20, 70, n_orders), rng.exponential(5.0, n_orders) + 0.1)
    approved = purchase + pd.to_timedelta(approve_h, unit="h")
    carrier = approved + pd.to_timedelta(np.clip(rng.gamma(2.0, 1.1, n_orders), 0.2, 20) * 24, unit="h")
    natural_days = rng.gamma(3.5, 2.2 * (0.6 + order_dist * 0.7), n_orders) + 2.0
    est_days = np.clip(np.round(11 + 6.5 * order_dist + rng.gamma(3, 1.8, n_orders)), 7, 60)
    estimated = (purchase + pd.to_timedelta(est_days, unit="D")).dt.normalize()
    p_late = np.clip(0.07 + 0.025 * (order_dist - 1.0), 0.04, 0.16)
    shock = rng.random(n_orders) < p_late
    delivered = purchase + pd.to_timedelta(natural_days, unit="D")
    # sem choque, a entrega fica dentro da estimativa (estimativas do Olist são conservadoras)
    within = estimated - pd.to_timedelta(rng.uniform(0.2, 3.0, n_orders), unit="D")
    delivered = pd.Series(np.where(~shock & (delivered > estimated), within, delivered), dtype="datetime64[ns]")
    delivered = pd.Series(np.where(shock, estimated + pd.to_timedelta(1.2 + rng.exponential(7.0, n_orders), unit="D"), delivered),
                          dtype="datetime64[ns]")
    delivered = pd.Series(np.maximum(delivered, carrier + pd.Timedelta(days=1)), dtype="datetime64[ns]")

    is_del = status == "delivered"
    has_approved = status != "created"
    has_carrier = np.isin(status, ["delivered", "shipped"])
    orders = pd.DataFrame({
        "order_id": order_ids, "customer_id": customers["customer_id"], "order_status": status,
        "order_purchase_timestamp": _fmt(purchase),
        "order_approved_at": _fmt(approved).where(has_approved),
        "order_delivered_carrier_date": _fmt(carrier).where(has_carrier),
        "order_delivered_customer_date": _fmt(delivered).where(is_del),
        "order_estimated_delivery_date": _fmt(estimated),
    })
    # defeito realista (existe no dado real): alguns pedidos 'delivered' sem data de entrega ao cliente
    broken = rng.choice(np.flatnonzero(is_del), size=max(3, n_orders // 3000), replace=False)
    orders.loc[broken, "order_delivered_customer_date"] = np.nan

    # --- itens/pagamentos: formato final -------------------------------------------------
    ship_limit = purchase.iloc[it["o"].to_numpy()].reset_index(drop=True) + pd.to_timedelta(rng.uniform(2, 6, len(it)), unit="D")
    order_items = pd.DataFrame({
        "order_id": [order_ids[o] for o in it["o"]], "order_item_id": it["order_item_id"].to_numpy(),
        "product_id": products["product_id"].to_numpy()[it["p"]], "seller_id": sellers["seller_id"].to_numpy()[it["s"]],
        "shipping_limit_date": _fmt(ship_limit), "price": price, "freight_value": freight,
    })
    order_payments = pd.DataFrame({
        "order_id": [order_ids[o] for o in pay["o"]], "payment_sequential": pay["payment_sequential"].to_numpy(),
        "payment_type": pay["payment_type"].to_numpy(), "payment_installments": pay["payment_installments"].to_numpy(),
        "payment_value": pay["payment_value"].round(2).to_numpy(),
    })

    # --- avaliações: nota correlacionada com atraso ----------------------------------------
    delay_days = (delivered.dt.normalize() - estimated).dt.days.to_numpy()
    score_w = {
        "ontime": _norm([5, 2, 6, 17, 70]), "late_short": _norm([30, 12, 15, 15, 28]),
        "late_long": _norm([55, 12, 10, 8, 15]), "undelivered": _norm([60, 5, 6, 9, 20]),
    }
    # qualidade latente: vendedores e categorias "melhores/piores" deslocam a nota (efeito pequeno)
    seller_q = rng.normal(0, 0.3, n_sellers)
    cat_q = rng.normal(0, 0.22, len(cat_names))
    first_item = it.drop_duplicates("o").set_index("o")
    order_shift = np.zeros(n_orders)
    idx = first_item.index.to_numpy()
    order_shift[idx] = seller_q[first_item["s"].to_numpy()] + cat_q[p_cat[first_item["p"].to_numpy()]]
    stars = np.array([1, 2, 3, 4, 5])
    review_rows = []
    rid = 0
    for i in range(n_orders):
        if rng.random() > (0.985 if is_del[i] else 0.6):
            continue
        if is_del[i]:
            kind = "ontime" if delay_days[i] <= 0 else ("late_short" if delay_days[i] <= 7 else "late_long")
            base = delivered.iloc[i]
        else:
            kind, base = "undelivered", approved.iloc[i] if has_approved[i] else purchase.iloc[i]
        n_rev = 2 if rng.random() < 0.005 else 1
        for r in range(n_rev):
            w = score_w[kind] * np.exp(0.5 * order_shift[i] * (stars - 3))
            score = int(rng.choice(stars, p=_norm(w)))
            created = (base + pd.Timedelta(days=float(rng.uniform(0, 3) + 2 * r))).normalize()
            answered = created + pd.Timedelta(days=float(rng.exponential(1.8) + 0.05))
            pool = POSITIVE if score >= 4 else NEGATIVE if score <= 2 else NEUTRAL
            msg = str(rng.choice(pool)) if rng.random() < 0.41 else None
            title = str(rng.choice(pool)).split(",")[0] if rng.random() < 0.12 else None
            review_rows.append((_hid("review", rid), order_ids[i], score, title, msg,
                                created.strftime("%Y-%m-%d %H:%M:%S"), answered.strftime("%Y-%m-%d %H:%M:%S")))
            rid += 1
    reviews = pd.DataFrame(review_rows, columns=[
        "review_id", "order_id", "review_score", "review_comment_title", "review_comment_message",
        "review_creation_date", "review_answer_timestamp"])

    return {
        "olist_customers_dataset": customers, "olist_orders_dataset": orders,
        "olist_order_items_dataset": order_items, "olist_order_payments_dataset": order_payments,
        "olist_order_reviews_dataset": reviews, "olist_products_dataset": products,
        "olist_sellers_dataset": sellers, "product_category_name_translation": translation,
    }


def write_csvs(tables: dict[str, pd.DataFrame], out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    for name, df in tables.items():
        df.to_csv(out / f"{name}.csv", index=False)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--orders", type=int, default=20000)
    ap.add_argument("--out", type=Path, default=PROJECT_DIR / "data" / "raw")
    ap.add_argument("--seed", type=int, default=SEED)
    args = ap.parse_args()
    tables = generate(args.orders, args.seed)
    write_csvs(tables, args.out)
    for name, df in tables.items():
        print(f"{name}: {len(df):,} linhas")


if __name__ == "__main__":
    main()
