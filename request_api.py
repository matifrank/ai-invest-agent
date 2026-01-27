import os
import requests
import json

def login():
    r = requests.post(
        "https://api.invertironline.com/token",
        data={
            "username": os.environ["IOL_USERNAME"],
            "password": os.environ["IOL_PASSWORD"],
            "grant_type": "password",
        },
        timeout=15,
    )
    r.raise_for_status()
    return r.json()["access_token"]

def get_quote(token: str):
    r = requests.get(
        "https://api.invertironline.com/api/v2/bcba/Titulos/VIST/Cotizacion",
        headers={"Authorization": f"Bearer {token}"},
        timeout=15,
    )
    r.raise_for_status()
    return r.json()

if __name__ == "__main__":
    token = login()
    quote = get_quote(token)

    # IMPORTANTE: solo imprime el JSON de la cotización
    print(json.dumps(quote, ensure_ascii=False, indent=2))
