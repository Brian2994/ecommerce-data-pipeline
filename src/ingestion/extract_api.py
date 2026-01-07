import requests
import json
from datetime import datetime
from pathlib import Path

BASE_URL = "https://fakestoreapi.com"
RAW_PATH = Path("data/raw")

RAW_PATH.mkdir(parents=True, exist_ok=True)


def extract(endpoint: str) -> list:
    """Extrai dados de um endpoint da API"""
    url = f"{BASE_URL}/{endpoint}"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def save_raw(data: list, endpoint: str) -> None:
    """Salva dados brutos com timestamp"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = RAW_PATH / f"{endpoint}_{timestamp}.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main():
    endpoints = ["products", "users", "carts"]

    for endpoint in endpoints:
        print(f"Extraindo {endpoint}...")
        data = extract(endpoint)
        save_raw(data, endpoint)
        print(f"{endpoint} salvo com sucesso!")

    

if __name__ == "__main__":
    main()