# test.py
# ------------------------------------------------------------
# Test de la requête L : SQL (SQLite) vs MongoDB
# ------------------------------------------------------------
import os
import sqlite3

import pandas as pd
import pymongo
from pymongo.errors import PyMongoError

# ------------------------------------------------------------
# Constantes (alignées sur app.py)
# ------------------------------------------------------------
DOSSIER_DATA = "data"
DOSSIER_SQLITE = "sqlite"
DB_FILE = os.path.join(DOSSIER_DATA, DOSSIER_SQLITE, "db", "paris2055.sqlite")

MONGO_URI = "mongodb://127.0.0.1:27017/"
MONGO_DB_NAME = "Paris2055"


# ------------------------------------------------------------
# Requête L en SQL / SQLite
# ------------------------------------------------------------
SQL_REQUETE_L = """
SELECT L.nom_ligne,
       COUNT(V.id_vehicule) AS total_vehicules,
       SUM(CASE WHEN V.type_vehicule = 'Electrique' THEN 1 ELSE 0 END)
           AS nb_electriques,
       (CAST(SUM(CASE WHEN V.type_vehicule = 'Electrique' THEN 1 ELSE 0 END) AS REAL)
        / COUNT(V.id_vehicule)) * 100
           AS pourcentage_electrique
FROM Ligne AS L
JOIN Vehicule AS V ON L.id_ligne = V.id_ligne
WHERE L.type = 'Bus'
GROUP BY L.nom_ligne
ORDER BY pourcentage_electrique DESC;
"""


def executer_requete_L_sqlite() -> pd.DataFrame:
    """
    Exécute la requête L sur la base SQLite et renvoie un DataFrame.
    """
    if not os.path.exists(DB_FILE):
        raise FileNotFoundError(f"Fichier SQLite introuvable : {DB_FILE}")

    with sqlite3.connect(DB_FILE) as conn:
        df_sql = pd.read_sql_query(SQL_REQUETE_L, conn)

    return df_sql


# ------------------------------------------------------------
# Requête L en MongoDB (copie de app.py)
# ------------------------------------------------------------
def query_L_mongo(db) -> pd.DataFrame:
    """
    Requête L (MongoDB) - corrigée (dédoublonnage des véhicules).
    Objectif : % de véhicules électriques sur les lignes de Bus.
    Résultat : nom_ligne, total_vehicules, nb_electriques, pourcentage_electrique
    """
    pipeline = [
        {"$match": {"type": "Bus"}},
        {"$unwind": "$arrets"},
        {"$unwind": "$arrets.horaires"},

        {"$match": {"arrets.horaires.vehicule.id_vehicule": {"$ne": None}}},

        # Dédoublonnage des véhicules (un véhicule unique par ligne)
        {
            "$group": {
                "_id": {
                    "nom_ligne": "$nom_ligne",
                    "id_vehicule": "$arrets.horaires.vehicule.id_vehicule",
                },
                "type_vehicule": {
                    "$first": "$arrets.horaires.vehicule.type_vehicule"
                },
            }
        },

        # Comptage des véhicules uniques par ligne
        {
            "$group": {
                "_id": "$_id.nom_ligne",
                "total_vehicules": {"$sum": 1},
                "nb_electriques": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$type_vehicule", "Electrique"]},
                            1,
                            0,
                        ]
                    }
                },
            }
        },
        {
            "$addFields": {
                "pourcentage_electrique": {
                    "$cond": [
                        {"$eq": ["$total_vehicules", 0]},
                        0,
                        {
                            "$multiply": [
                                {
                                    "$divide": [
                                        "$nb_electriques",
                                        "$total_vehicules",
                                    ]
                                },
                                100,
                            ]
                        },
                    ]
                },
            }
        },
        {"$sort": {"pourcentage_electrique": -1}},
        {
            "$project": {
                "_id": 0,
                "nom_ligne": "$_id",
                "total_vehicules": 1,
                "nb_electriques": 1,
                "pourcentage_electrique": 1,
            }
        },
    ]

    docs = list(db.lignes.aggregate(pipeline))
    if not docs:
        return pd.DataFrame(
            columns=[
                "nom_ligne",
                "total_vehicules",
                "nb_electriques",
                "pourcentage_electrique",
            ]
        )

    df = pd.DataFrame(docs)
    # On force l'ordre des colonnes pour faciliter la comparaison
    cols = ["nom_ligne", "total_vehicules", "nb_electriques", "pourcentage_electrique"]
    return df[cols]


# ------------------------------------------------------------
# Affichage de debug
# ------------------------------------------------------------
def afficher_debug(df: pd.DataFrame, titre: str, n: int = 10) -> None:
    """
    Affiche quelques informations de debug sur un DataFrame :
    shape, colonnes, dtypes, head(n).
    """
    print()
    print("=" * 80)
    print(titre)
    print("=" * 80)

    if df is None:
        print("DataFrame : None")
        return

    print(f"Shape : {df.shape}")
    print("Colonnes :", list(df.columns))
    print("Types :")
    print(df.dtypes)
    print()
    print(f"Head({n}) :")
    print(df.head(n))
    print()


def main() -> None:
    # --------------------------------------------------------
    # 1) Requête L côté SQLite
    # --------------------------------------------------------
    print("### TEST REQUÊTE L - SQL vs MongoDB ###")

    try:
        df_sql_L = executer_requete_L_sqlite()
    except Exception as exc:
        print("[ERREUR SQL] Impossible d'exécuter la requête L sur SQLite :", exc)
        df_sql_L = None

    afficher_debug(df_sql_L, "Résultat SQLite - Requête L (SQL)")

    # --------------------------------------------------------
    # 2) Requête L côté MongoDB
    # --------------------------------------------------------
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        client.admin.command("ping")
    except PyMongoError as exc:
        print("[ERREUR MONGO] Impossible de se connecter à MongoDB :", exc)
        return

    try:
        if MONGO_DB_NAME not in client.list_database_names():
            print(
                f"[ERREUR MONGO] La base '{MONGO_DB_NAME}' n'existe pas. "
                "Lance d'abord la migration (Partie 2 de l'application)."
            )
            client.close()
            return

        db = client[MONGO_DB_NAME]
        df_mongo_L = query_L_mongo(db)

    except Exception as exc:
        print("[ERREUR MONGO] Erreur pendant l'exécution de la requête L :", exc)
        df_mongo_L = None
    finally:
        client.close()

    afficher_debug(df_mongo_L, "Résultat MongoDB - Requête L (MongoDB)")

    print("### FIN DU TEST ###")


if __name__ == "__main__":
    main()
