"""
scripts/fetch_prices.py — Génère le fichier de prix journalier pour MTGbyNico-prices.
Télécharge le bulk Scryfall, extrait les prix EUR, sauvegarde prices/YYYY-MM-DD.json
et son hash SHA256. Supprime les fichiers plus anciens que RETENTION_DAYS.
Exécuté chaque nuit par GitHub Actions.
"""
import hashlib
import json
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import requests

SCRYFALL_BULK_API = "https://api.scryfall.com/bulk-data"
RETENTION_DAYS    = 180
PRICES_DIR        = Path(__file__).parent.parent / "prices"


def fetch_download_url() -> tuple[str, str]:
    """Retourne (download_url, updated_at) du bulk default_cards."""
    resp = requests.get(SCRYFALL_BULK_API, timeout=15)
    resp.raise_for_status()
    bulk_list = resp.json().get("data", [])
    entry = next((b for b in bulk_list if b["type"] == "default_cards"), None)
    if not entry:
        raise RuntimeError("Type 'default_cards' introuvable dans l'API Scryfall.")
    return entry["download_uri"], entry.get("updated_at", "")


def download_bulk(url: str) -> list:
    """Télécharge et parse le bulk JSON Scryfall. Retourne la liste des cartes."""
    print(f"Téléchargement : {url}", flush=True)
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        size_mb = int(r.headers.get("Content-Length", 0)) // (1024 * 1024)
        print(f"Taille estimée : {size_mb} Mo", flush=True)
        chunks = []
        downloaded = 0
        for chunk in r.iter_content(chunk_size=512 * 1024):
            if chunk:
                chunks.append(chunk)
                downloaded += len(chunk)
        content = b"".join(chunks)
    print(f"Téléchargé : {round(len(content) / 1024 / 1024, 1)} Mo", flush=True)
    return json.loads(content.decode("utf-8"))


def extract_eur_prices(cards: list) -> dict:
    """Extrait {scryfall_id: [eur, eur_foil, eur_etched]} pour les cartes avec prix EUR."""
    result = {}
    for card in cards:
        prices = card.get("prices") or {}
        eur        = _to_float(prices.get("eur"))
        eur_foil   = _to_float(prices.get("eur_foil"))
        eur_etched = _to_float(prices.get("eur_etched"))
        if eur is None and eur_foil is None and eur_etched is None:
            continue
        result[card["id"]] = [eur, eur_foil, eur_etched]
    return result


def _to_float(val) -> float | None:
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def save_price_file(date_str: str, cards: dict) -> Path:
    """Sauvegarde prices/YYYY-MM-DD.json et son fichier SHA256."""
    PRICES_DIR.mkdir(exist_ok=True)
    payload = {"date": date_str, "source": "scryfall", "cards": cards}
    content = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

    price_file = PRICES_DIR / f"{date_str}.json"
    price_file.write_bytes(content)

    sha256 = hashlib.sha256(content).hexdigest()
    sha_file = PRICES_DIR / f"{date_str}.sha256"
    sha_file.write_text(f"{sha256}  {date_str}.json\n", encoding="utf-8")

    print(f"Fichier sauvegardé : {price_file.name} ({len(content) // 1024} Ko, {len(cards)} cartes)", flush=True)
    return price_file


def prune_old_files():
    """Supprime les fichiers de prix plus anciens que RETENTION_DAYS."""
    cutoff = (date.today() - timedelta(days=RETENTION_DAYS)).isoformat()
    removed = 0
    for f in sorted(PRICES_DIR.glob("*.json")):
        date_str = f.stem
        if date_str < cutoff:
            f.unlink()
            sha = PRICES_DIR / f"{date_str}.sha256"
            if sha.exists():
                sha.unlink()
            removed += 1
    if removed:
        print(f"Purge : {removed} fichier(s) supprimé(s) (antérieurs au {cutoff})", flush=True)


def main():
    today = date.today().isoformat()
    target_file = PRICES_DIR / f"{today}.json"

    # Ne pas re-générer si déjà fait aujourd'hui
    if target_file.exists():
        print(f"Fichier {today}.json déjà présent, rien à faire.", flush=True)
        return 0

    try:
        download_url, updated_at = fetch_download_url()
        print(f"Bulk Scryfall mis à jour le : {updated_at}", flush=True)
        cards_raw  = download_bulk(download_url)
        prices     = extract_eur_prices(cards_raw)
        print(f"Cartes avec prix EUR : {len(prices)}", flush=True)
        save_price_file(today, prices)
        prune_old_files()
        return 0
    except Exception as e:
        print(f"ERREUR : {e}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
