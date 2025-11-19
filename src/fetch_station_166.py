import os
import sys
import requests
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

# --- Paramètres de la station cible ---
CONTRACT_NAME = "Toulouse"
STATION_NUMBER = 166
API_URL = f"https://api.jcdecaux.com/vls/v3/stations/{STATION_NUMBER}"

def get_api_key():
    """
    Récupère la clé d'API dans la variable d'environnement JCDECAUX_API_KEY.
    Si elle n'est pas définie, on arrête le programme proprement.
    """
    api_key = os.getenv("JCDECAUX_API_KEY")
    if not api_key:
        print("Erreur : variable d'environnement JCDECAUX_API_KEY non définie.", file=sys.stderr)
        sys.exit(1)
    return api_key

def fetch_station(contract_name: str, api_key: str) -> dict:
    """
    Interroge l'API JCDecaux pour la station STATION_NUMBER du contrat donné.
    Retourne la réponse JSON (un dict Python).
    """
    params = {
        "contract": contract_name,
        "apiKey": api_key
    }
    headers = {
        "Accept": "application/json",
        "User-Agent": "toulouse-bikes-timeseries (student project)"
    }

    resp = requests.get(API_URL, params=params, headers=headers, timeout=10)

    if resp.status_code != 200:
        print(f"Erreur API {resp.status_code} : {resp.text}", file=sys.stderr)
        sys.exit(1)

    return resp.json()

def flatten_station(station: dict, collection_time_iso: str) -> dict:
    """
    Transforme la structure JSON imbriquée de la station
    en un dictionnaire "plat" avec des colonnes simples.

    On distingue bien :
    - collection_time : moment où TOI tu as collecté les données
    - lastUpdate : moment de la dernière mise à jour côté JCDecaux
    """
    pos = station.get("position", {}) or {}
    total_stands = station.get("totalStands", {}) or {}
    total_av = total_stands.get("availabilities", {}) or {}

    return {
        "collection_time": collection_time_iso,        # timestamp de ta collecte
        "lastUpdate": station.get("lastUpdate"),       # timestamp JCDecaux
        "number": station.get("number"),
        "contractName": station.get("contractName"),
        "name": station.get("name"),
        "address": station.get("address"),
        "latitude": pos.get("latitude"),
        "longitude": pos.get("longitude"),
        "status": station.get("status"),              # OPEN / CLOSED
        "connected": station.get("connected"),        # True / False
        # Comptages
        "total_capacity": total_stands.get("capacity"),
        "total_bikes": total_av.get("bikes"),
        "total_stands_free": total_av.get("stands"),
        "total_mech_bikes": total_av.get("mechanicalBikes"),
        "total_elec_bikes": total_av.get("electricalBikes"),
    }

def main():
    # 1) Récupérer la clé
    api_key = get_api_key()

    # 2) Appeler l'API pour la station 166
    station_json = fetch_station(CONTRACT_NAME, api_key)

    # 3) Timestamp de collecte en UTC (important pour la série temporelle)
    collection_time = datetime.now(timezone.utc).isoformat()

    # 4) Aplatir les données pour en faire une seule ligne de tableau
    row = flatten_station(station_json, collection_time)
    df = pd.DataFrame([row])

    # 5) Sauvegarder / ajouter la ligne dans un CSV
    out_dir = Path("data") / "dynamic"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "station_166_rangueil_metro.csv"

    # Si le fichier existe déjà → on ajoute une ligne sans réécrire l'en-tête
    if out_file.exists():
        df.to_csv(out_file, mode="a", header=False, index=False)
    else:
        # première fois → on écrit aussi les noms de colonnes
        df.to_csv(out_file, index=False)

    print(f"Une nouvelle observation ajoutée à {out_file}")

if __name__ == "__main__":
    main()
