from google.cloud import bigquery
from pathlib import Path

PROJECT_ID = "projetos-de-teste-445920"
DATASET = "ecommerce_trusted"
TRUSTED_PATH = Path("data/trusted")

client = bigquery.Client(project=PROJECT_ID)