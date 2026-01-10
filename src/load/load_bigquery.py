from google.cloud import bigquery
from pathlib import Path

PROJECT_ID = "projetos-de-teste-445920"
DATASET = "ecommerce_trusted"
TRUSTED_PATH = Path("data/trusted")

client = bigquery.Client(project=PROJECT_ID)

SCHEMAS = {
    "orders_items": [
        bigquery.SchemaField("order_id", "INT64"),
        bigquery.SchemaField("user_id", "INT64"),
        bigquery.SchemaField("product_id", "INT64"),
        bigquery.SchemaField("quantity", "INT64"),
        bigquery.SchemaField("order_date", "TIMESTAMP"),
    ],
    "products": [
        bigquery.SchemaField("product_id", "INT64"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("price", "FLOAT64"),
    ],
    "users": [
        bigquery.SchemaField("user_id", "INT64"),
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("username", "STRING"),
        bigquery.SchemaField("full_name", "STRING"),
    ],
}

def load_parquet(table_name: str, file_path: Path):
    table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition="WRITE_TRUNCATE", # WRITE_APPEND
        schema=SCHEMAS[table_name]
    )

    with open(file_path, "rb") as f:
        job = client.load_table_from_file(
            f,
            table_id,
            job_config=job_config
        )

    job.result()
    print(f"Tabela {table_name} carregada com sucesso!")

def main():
    load_parquet("products", TRUSTED_PATH / "products.parquet")
    load_parquet("users", TRUSTED_PATH / "users.parquet")
    load_parquet("orders_items", TRUSTED_PATH / "orders_items.parquet")

if __name__ == "__main__":
    main()