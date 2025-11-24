import os
import sqlite3
import pandas as pd
import pymongo

# ====== CONFIG ======
DOSSIER_DATA = "data"
DOSSIER_SQLITE = "sqlite"
DB_FILE = os.path.join(DOSSIER_DATA, DOSSIER_SQLITE, "db", "paris2055.sqlite")

MONGO_URI = "mongodb://127.0.0.1:27017/"
MONGO_DB_NAME = "Paris2055"


# ====== OUTIL COMMUN ======
def aggregate_to_df(collection, pipeline) -> pd.DataFrame:
    try:
        docs = list(collection.aggregate(pipeline))
    except Exception as e:
        print(f"Erreur Mongo: {e}")
        return pd.DataFrame()
    
    if not docs:
        return pd.DataFrame()
    return pd.DataFrame(docs)


# ====== VERIFICATION DE LA MIGRATION ======
def check_migration_quality(db):
    print("\n🕵️‍♂️ --- VÉRIFICATION DE LA QUALITÉ DE MIGRATION ---")
    
    # On cherche un véhicule au hasard pour voir s'il a le champ magique
    pipeline = [
        {"$unwind": "$arrets"},
        {"$unwind": "$arrets.horaires"},
        {"$match": {"arrets.horaires.vehicule.id_vehicule": {"$ne": None}}},
        {"$limit": 1},
        {"$project": {"vehicule": "$arrets.horaires.vehicule"}}
    ]
    
    res = list(db.lignes.aggregate(pipeline))
    
    if not res:
        print("❌ Aucun véhicule trouvé dans la collection 'lignes'.")
        return False

    vehicule = res[0]['vehicule']
    # print(f"Exemple de véhicule trouvé : {vehicule}")
    
    if "id_ligne_officielle" in vehicule:
        print(f"✅ LE CHAMP 'id_ligne_officielle' EST PRÉSENT ! (Valeur: {vehicule['id_ligne_officielle']})")
        return True
    else:
        print("❌ LE CHAMP 'id_ligne_officielle' EST MANQUANT !")
        return False


# ====== REQUETE D MONGO (VERSION V5 - LOOKUP INVERSÉ) ======
def query_D_mongo(db, debug: bool = False) -> pd.DataFrame:
    """
    Requête D (MongoDB) - Version V5 (Lookup Inversé).
    Objectif : Récupérer la moyenne CO2 basée sur l'ID officiel du véhicule.
    """
    if debug:
        print("[D][Mongo] Démarrage du pipeline V5 (Lookup Inversé)...")

    pipeline = [
        # 1. On descend directement aux véhicules (peu importe la ligne porteuse)
        {"$unwind": "$arrets"},
        {"$unwind": "$arrets.horaires"},
        
        # 2. On ne garde que les véhicules valides avec un ID officiel
        {"$match": {
            "arrets.horaires.vehicule.id_vehicule": {"$ne": None},
            "arrets.horaires.vehicule.id_ligne_officielle": {"$ne": None}
        }},

        # 3. Projection simplifiée pour préparer le Lookup
        {
            "$project": {
                "_id": 0,
                "id_vehicule": "$arrets.horaires.vehicule.id_vehicule",
                "immatriculation": "$arrets.horaires.vehicule.immatriculation",
                # C'est ici l'astuce : on prépare la clé de jointure
                "target_line_id": "$arrets.horaires.vehicule.id_ligne_officielle"
            }
        },

        # 4. LE LOOKUP CORRECTEUR : On va chercher le CO2 de la ligne OFFICIELLE
        # et non pas le CO2 de la ligne racine du document.
        {
            "$lookup": {
                "from": "capteurs",
                "localField": "target_line_id",   # On utilise l'ID officiel du véhicule
                "foreignField": "arret.id_ligne", # On cherche les capteurs liés à cette ligne
                "pipeline": [
                    {"$match": {"type_capteur": "CO2"}},
                    {"$unwind": "$mesures"},
                    {"$group": {"_id": None, "avg": {"$avg": "$mesures.valeur"}}}
                ],
                "as": "co2_data"
            }
        },
        
        # On ne garde que les véhicules dont la ligne officielle a des capteurs CO2 (Inner Join)
        {"$unwind": "$co2_data"}, 

        # 5. Groupement final (Dédoublonnage)
        # Comme le véhicule existe en 59 exemplaires dans la base, ils auront maintenant 
        # tous la même moyenne (celle de la ligne officielle). On en garde un seul.
        {
            "$group": {
                "_id": {
                    "id": "$id_vehicule",
                    "immat": "$immatriculation"
                },
                "moyenne_co2": {"$first": "$co2_data.avg"}
            }
        },

        # 6. Formatage final
        {
            "$project": {
                "_id": 0,
                "id_vehicule": "$_id.id",
                "immatriculation": "$_id.immat",
                "moyenne_co2": 1
            }
        },
        {"$sort": {"moyenne_co2": -1}},
    ]

    df = aggregate_to_df(db.lignes, pipeline)
    
    if debug:
        print(f"[D][Mongo] Pipeline terminé. {len(df)} lignes trouvées.")

    if df.empty:
        return pd.DataFrame(columns=["id_vehicule", "immatriculation", "moyenne_co2"])
        
    return df[["id_vehicule", "immatriculation", "moyenne_co2"]]


# ====== DEBUG DIRECT D : SQL vs MONGO ======
def debug_compare_D_direct() -> None:
    print("\n================= DEBUG REQUETE D (SQL vs Mongo) =================\n")
    
    # ---------- PRE-CHECK MONGO ----------
    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    
    migration_ok = check_migration_quality(db)
    if not migration_ok:
        print("\n⛔ ARRET DU TEST : La base MongoDB n'est pas à jour.")
        client.close()
        return

    # ---------- 1) SQL D ----------
    print("\n[ETAPE 1] Calcul D côté SQL...")
    sql_D = (
        "SELECT V.id_vehicule, V.immatriculation, "
        "      AVG(M.valeur) AS moyenne_co2 "
        "FROM Vehicule AS V "
        "JOIN Ligne AS L ON V.id_ligne = L.id_ligne "
        "JOIN Arret AS A ON L.id_ligne = A.id_ligne "
        "JOIN Capteur AS C ON A.id_arret = C.id_arret "
        "JOIN Mesure AS M ON C.id_capteur = M.id_capteur "
        "WHERE C.type_capteur = 'CO2' "
        "GROUP BY V.id_vehicule, V.immatriculation "
        "ORDER BY moyenne_co2 DESC;"
    )

    with sqlite3.connect(DB_FILE) as conn:
        df_sql = pd.read_sql_query(sql_D, conn)
    print(f"[D][SQL] Terminé. {len(df_sql)} lignes.")

    # ---------- 2) Mongo D ----------
    print("[ETAPE 2] Calcul D côté MongoDB...")
    df_mongo = query_D_mongo(db, debug=True)
    client.close()

    # ---------- 3) Comparaison ----------
    print("\n[ETAPE 3] Comparaison des résultats...")

    if df_sql.empty or df_mongo.empty:
        print("ATTENTION: L'un des DataFrames est vide.")
        return

    df_merged = df_sql.merge(
        df_mongo,
        on=["id_vehicule", "immatriculation"],
        how="outer",
        suffixes=("_sql", "_mongo"),
    )

    df_merged["diff"] = (df_merged["moyenne_co2_sql"] - df_merged["moyenne_co2_mongo"]).abs()
    
    # Tolérance flottante
    TOLERANCE = 0.0000001
    nb_erreurs = df_merged[df_merged["diff"] > TOLERANCE].shape[0]

    print(f"Nombre total de lignes fusionnées : {len(df_merged)}")
    print(f"Nombre de différences significatives : {nb_erreurs}")
    
    if nb_erreurs == 0:
        if len(df_sql) == len(df_mongo):
             print("\n✅ SUCCÈS TOTAL : Les résultats sont mathématiquement identiques (Taille + Valeurs) !")
        else:
             print(f"\n⚠️ VALEURS OK MAIS TAILLE DIFFÉRENTE (SQL={len(df_sql)} vs Mongo={len(df_mongo)})")
    else:
        print(f"\n❌ ECHEC : Il y a {nb_erreurs} différences.")
        print("Top 5 des différences :")
        print(df_merged.sort_values("diff", ascending=False).head(5))

    print("\n================= FIN DEBUG =================\n")


if __name__ == "__main__":
    debug_compare_D_direct()