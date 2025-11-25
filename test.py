import sqlite3
import pandas as pd
import os

# Chemin vers ta base (vérifie qu'il correspond bien à ton dossier)
DB_FILE = "data/sqlite/db/paris2055.sqlite"

def inspecter_la_base():
    if not os.path.exists(DB_FILE):
        print(f"❌ ERREUR : Le fichier {DB_FILE} est introuvable.")
        print("Vérifie le chemin dans le script.")
        return

    print(f"🔍 INSPECTION DE LA BASE : {DB_FILE}")
    print("=" * 60)

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # 1. Récupérer la liste de toutes les tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]

    # Mots-clés pour repérer la localisation
    mots_cles_loc = ['lat', 'lon', 'geo', 'coord', 'position', 'quartier', 'nom']

    for table in tables:
        print(f"\n📂 TABLE : {table.upper()}")
        print("-" * 30)

        # 2. Lire 3 lignes pour voir les données
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table} LIMIT 3", conn)
            
            if df.empty:
                print("⚠️  Table vide.")
            else:
                # Affichage des colonnes avec un indicateur si ça ressemble à de la localisation
                print("Colonnes détectées :")
                for col in df.columns:
                    marker = "📍" if any(x in col.lower() for x in mots_cles_loc) else "  "
                    print(f"{marker} {col}")
                
                print("\n👀 Aperçu des données :")
                print(df.to_string(index=False))
                
        except Exception as e:
            print(f"Erreur de lecture : {e}")
            
        print("=" * 60)

    conn.close()

if __name__ == "__main__":
    inspecter_la_base()