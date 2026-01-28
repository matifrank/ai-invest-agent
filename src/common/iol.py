import time
import requests
from typing import Optional, Dict, Any, Tuple

IOL_BASE = "https://api.invertironline.com"


class IOLClient:
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.expires_at: float = 0.0

    def login_password(self):
        r = requests.post(
            f"{IOL_BASE}/token",
            data={"username": self.username, "password": self.password, "grant_type": "password"},
            timeout=20,
        )
        r.raise_for_status()
        j = r.json()
        self.access_token = j.get("access_token")
        self.refresh_token = j.get("refresh_token")
        expires_in = float(j.get("expires_in", 900))
        self.expires_at = time.time() + expires_in - 20

    def refresh(self):
        if not self.refresh_token:
            return self.login_password()
        r = requests.post(
            f"{IOL_BASE}/token",
            data={"refresh_token": self.refresh_token, "grant_type": "refresh_token"},
            timeout=20,
        )
        if r.status_code >= 400:
            return self.login_password()
        j = r.json()
        self.access_token = j.get("access_token")
        self.refresh_token = j.get("refresh_token", self.refresh_token)
        expires_in = float(j.get("expires_in", 900))
        self.expires_at = time.time() + expires_in - 20

    def ensure_token(self):
        if not self.access_token or time.time() >= self.expires_at:
            if self.refresh_token:
                self.refresh()
            else:
                self.login_password()

    def headers(self) -> Dict[str, str]:
        self.ensure_token()
        return {"Authorization": f"Bearer {self.access_token}"}

    def get_quote(self, mercado: str, simbolo: str) -> Optional[Dict[str, Any]]:
        url = f"{IOL_BASE}/api/v2/{mercado}/Titulos/{simbolo}/Cotizacion"
        r = requests.get(url, headers=self.headers(), timeout=20)
        if r.status_code == 401:
            self.refresh()
            r = requests.get(url, headers=self.headers(), timeout=20)
        if r.status_code >= 400:
            return None
        return r.json()


def _safe_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except:
        return None


def parse_iol_quote(q: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normaliza lo más útil del JSON de IOL.
    """
    last = _safe_float(q.get("ultimoPrecio"))
    plazo = q.get("plazo")  # e.g. "T1"
    monto = _safe_float(q.get("montoOperado"))
    vol = _safe_float(q.get("volumenNominal"))

    bid = ask = bid_qty = ask_qty = None
    puntas = q.get("puntas") or []
    if isinstance(puntas, list) and puntas and isinstance(puntas[0], dict):
        bid = _safe_float(puntas[0].get("precioCompra"))
        ask = _safe_float(puntas[0].get("precioVenta"))
        bid_qty = _safe_float(puntas[0].get("cantidadCompra"))
        ask_qty = _safe_float(puntas[0].get("cantidadVenta"))

    return {
        "last": last,
        "bid": bid,
        "ask": ask,
        "bid_qty": bid_qty,
        "ask_qty": ask_qty,
        "plazo": plazo,
        "montoOperado": monto,
        "volumenNominal": vol,
        "raw": q,
    }


def get_last_price(iol: IOLClient, mercado: str, simbolo: str) -> Optional[float]:
    q = iol.get_quote(mercado, simbolo)
    if not q:
        return None
    p = parse_iol_quote(q)
    if p["last"] is not None:
        return p["last"]
    if p["bid"] is not None and p["ask"] is not None:
        return (p["bid"] + p["ask"]) / 2.0
    return None
