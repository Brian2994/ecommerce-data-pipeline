import pandas as pd
from pathlib import Path

RAW_PATH = Path("data/raw")
TRUSTED_PATH = Path("data/trusted")

TRUSTED_PATH.mkdir(parents=True, exist_ok=True)

products_file = max(RAW_PATH.glob("products_*.json"))
users_file = max(RAW_PATH.glob("users_*.json"))
carts_file = max(RAW_PATH.glob("carts_*.json"))

df_products = pd.read_json(products_file)
df_users = pd.read_json(users_file)
df_carts = pd.read_json(carts_file)

# Products
df_products = df_products.rename(columns={
    "id": "product_id"
})

df_products["price"] = df_products["price"].astype(float)

df_products = df_products[[
    "product_id",
    "title",
    "category",
    "price"
]]

# Users
df_users = df_users.rename(columns={
    "id": "user_id"
})

df_users["full_name"] = df_users["name"].apply(
    lambda x: f"{x['firstname']} {x['lastname']}"
)

df_users = df_users[[
    "user_id",
    "email",
    "username",
    "full_name"
]]

# Orders
df_orders = df_carts.rename(columns={
    "id": "order_id",
    "userId": "user_id"
})

df_orders["order_date"] = (
    pd.to_datetime(df_orders["date"], utc=True)
    .dt.tz_convert("UTC") # garante UTC
    .dt.tz_localize(None) # remove timezone explicitamente
    .astype("datetime64[us]") # converte para microssegundos
)

df_orders_items = df_orders.explode("products")
df_orders_items["product_id"] = df_orders_items["products"].apply(lambda x: x["productId"])
df_orders_items["quantity"] = df_orders_items["products"].apply(lambda x: x["quantity"])

df_orders_items = df_orders_items[[
    "order_id",
    "user_id",
    "product_id",
    "quantity",
    "order_date"
]]

# Save
df_products.to_parquet(TRUSTED_PATH / "products.parquet", index=False)
df_users.to_parquet(TRUSTED_PATH / "users.parquet", index=False)
df_orders_items.to_parquet(TRUSTED_PATH / "orders_items.parquet", index=False)