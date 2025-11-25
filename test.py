import sqlite3
import pandas as pd
import pymongo
import os
import time

# --- CONFIGURATION ---
DB_SQLITE = "data/sqlite/db/paris2055.sqlite"  # Vérifie ton chemin
MONGO_URI = "mongodb://127.0.0.1:27017/"
MONGO_DB_NAME = "Paris2055_2"  # Une base de test séparée

def reset_mongo_db():
    """Supprime la base de test pour repartir de zéro."""
    client = pymongo.MongoClient(MONGO_URI)
    client.drop_database(MONGO_DB_NAME)
    print(f"🗑️  Base MongoDB '{MONGO_DB_NAME}' supprimée pour test propre.")
    client.close()

def migration_speciale_k():
    """
    Migration simplifiée mais CORRIGÉE : 
    Elle injecte 'nom_chauffeur' directement dans 'vehicules_cache' des lignes.
    """
    print("🚀 Démarrage de la migration optimisée (Lignes + Trafic + Véhicules)...")
    
    if not os.path.exists(DB_SQLITE):
        print(f"❌ ERREUR : Base SQLite introuvable ici : {DB_SQLITE}")
        return

    conn = sqlite3.connect(DB_SQLITE)
    
    # 1. Chargement des données brutes
    df_ligne = pd.read_sql("SELECT * FROM Ligne", conn)
    df_trafic = pd.read_sql("SELECT * FROM Trafic", conn)
    df_vehicule = pd.read_sql("SELECT * FROM Vehicule", conn)
    df_chauffeur = pd.read_sql("SELECT * FROM Chauffeur", conn)
    conn.close()

    # 2. Préparation du Cache Véhicule AVEC Chauffeur (La Clé du succès)
    # On joint Véhicules et Chauffeurs AVANT de construire les objets
    print("⚙️  Construction du cache véhicules avec chauffeurs...")
    df_v_merged = df_vehicule.merge(df_chauffeur, on="id_chauffeur", how="left")
    
    # Dictionnaire : id_ligne -> Liste de véhicules (avec nom chauffeur)
    vehicules_map = {}
    for id_ligne, group in df_v_merged.groupby("id_ligne"):
        v_list = []
        for _, row in group.iterrows():
            v_doc = {
                "id_vehicule": int(row["id_vehicule"]),
                "immatriculation": row["immatriculation"],
                # C'EST ICI QUE TOUT SE JOUE : On stocke le nom direct
                "nom_chauffeur": row["nom"] if pd.notnull(row["nom"]) else None
            }
            v_list.append(v_doc)
        vehicules_map[int(id_ligne)] = v_list

    # 3. Préparation du Trafic
    print("⚙️  Association du trafic aux lignes...")
    trafic_map = {}
    for id_ligne, group in df_trafic.groupby("id_ligne"):
        t_list = []
        for _, row in group.iterrows():
            # On ne garde que ce qui sert au calcul (retard)
            t_doc = {
                "id_trafic": int(row["id_trafic"]),
                "retard_minutes": int(row["retard_minutes"]) if pd.notnull(row["retard_minutes"]) else 0
            }
            t_list.append(t_doc)
        trafic_map[int(id_ligne)] = t_list

    # 4. Assemblage et Insertion MongoDB
    documents = []
    for _, row in df_ligne.iterrows():
        id_l = int(row["id_ligne"])
        doc = {
            "id_ligne": id_l,
            "nom_ligne": row["nom_ligne"],
            "type": row["type"],
            # Injection des caches
            "vehicules_cache": vehicules_map.get(id_l, []),
            "trafic": trafic_map.get(id_l, [])
        }
        documents.append(doc)

    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    if documents:
        db.lignes.insert_many(documents)
        print(f"✅ {len(documents)} lignes migrées dans '{MONGO_DB_NAME}.lignes'.")
    client.close()

def get_sql_result():
    print("\n🔵 [SQL] Exécution Requête K...")
    query = """
    SELECT C.nom, 
           AVG(T.retard_minutes) AS moyenne_retard_minutes
    FROM Chauffeur AS C
    JOIN Vehicule AS V ON C.id_chauffeur = V.id_chauffeur
    JOIN Trafic AS T ON V.id_ligne = T.id_ligne
    GROUP BY C.nom
    ORDER BY moyenne_retard_minutes DESC;
    """
    with sqlite3.connect(DB_SQLITE) as conn:
        df = pd.read_sql_query(query, conn)
    return df

def get_mongo_result():
    print("🟢 [MONGO] Exécution Requête K (Version Finale)...")
    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]

    pipeline = [
        # 1. On ne garde que les lignes avec trafic
        { "$match": { "trafic.retard_minutes": { "$exists": True } } },

        # 2. Pré-calcul stats ligne
        { "$addFields": {
            "line_sum": { "$sum": "$trafic.retard_minutes" },
            "line_count": { "$size": "$trafic" }
        }},

        # 3. On déroule les véhicules (qui contiennent maintenant le chauffeur !)
        { "$unwind": "$vehicules_cache" },

        # 4. On garde les véhicules avec chauffeurs
        { "$match": { "vehicules_cache.nom_chauffeur": { "$ne": None } } },

        # 5. Groupement (Imitation parfaite du JOIN SQL)
        { "$group": {
            "_id": "$vehicules_cache.nom_chauffeur",
            "total_delay": { "$sum": "$line_sum" },
            "total_trips": { "$sum": "$line_count" }
        }},

        # 6. Moyenne
        { "$project": {
            "_id": 0,
            "nom": "$_id",
            "moyenne_retard_minutes": { "$divide": ["$total_delay", "$total_trips"] }
        }},
        { "$sort": { "moyenne_retard_minutes": -1 } }
    ]

    results = list(db.lignes.aggregate(pipeline))
    client.close()
    return pd.DataFrame(results)

def comparer(df_sql, df_mongo):
    print("\n📊 --- COMPARATIF ---")
    
    # Normalisation
    df_sql = df_sql.sort_values("nom").reset_index(drop=True)
    df_mongo = df_mongo.sort_values("nom").reset_index(drop=True)

    print(f"Lignes SQL   : {len(df_sql)}")
    print(f"Lignes Mongo : {len(df_mongo)}")

    if df_sql.empty or df_mongo.empty:
        print("❌ Un des résultats est vide !")
        return

    try:
        # Tolérance très faible (1e-5) pour valider l'exactitude mathématique
        pd.testing.assert_frame_equal(df_sql, df_mongo, check_dtype=False, rtol=1e-5)
        print("\n✨ SUCCÈS : LES RÉSULTATS SONT STRICTEMENT IDENTIQUES ! ✨")
        print(df_mongo.head(3))
    except AssertionError:
        print("\n❌ ÉCHEC : Différences trouvées.")
        merged = pd.merge(df_sql, df_mongo, on="nom", suffixes=('_sql', '_mongo'), how='outer')
        merged['diff'] = abs(merged['moyenne_retard_minutes_sql'] - merged['moyenne_retard_minutes_mongo'])
        print(merged[merged['diff'] > 0.0001].head())

if __name__ == "__main__":
    reset_mongo_db()
    migration_speciale_k()
    df_sql = get_sql_result()
    df_mongo = get_mongo_result()
    comparer(df_sql, df_mongo)