import os
import sqlite3
import json
import time
from typing import Dict, List, Optional, Any

import pandas as pd
import pymongo
import streamlit as st
from groq import Groq
from dotenv import load_dotenv, set_key

# ======================================================================
# CONFIGURATION ET CONSTANTES
# ======================================================================

load_dotenv()

# Chemins
DOSSIER_DATA = "data"
DB_FILE = os.path.join(DOSSIER_DATA, "sqlite", "db", "paris2055.sqlite")

# MongoDB
MONGO_URI = "mongodb://127.0.0.1:27017/"
MONGO_DB_NAME = "Paris2055"

# IA
SCHEMA_CONTEXT = """
Tu es un expert MongoDB et Python. Ton but est de traduire une question naturelle en pipeline d'agrégation MongoDB.
(Le schéma reste inchangé par rapport à ta version précédente...)
"""

# ======================================================================
# UTILITAIRES (Fonctions pures)
# ======================================================================

def parse_geojson_geometry(geojson_str: Optional[str]) -> Optional[Dict]:
    """Extrait la géométrie d'une chaîne GeoJSON."""
    if not isinstance(geojson_str, str) or not geojson_str.strip():
        return None
    try:
        obj = json.loads(geojson_str)
        if "type" in obj and "coordinates" in obj:
            return {"type": obj["type"], "coordinates": obj["coordinates"]}
        if "geometry" in obj:
            return obj["geometry"]
    except Exception:
        pass
    return None

def infer_unite_from_type(type_capteur: str) -> str:
    """Devine l'unité selon le type de capteur."""
    if not isinstance(type_capteur, str): return ""
    t = type_capteur.lower()
    if "bruit" in t: return "dB"
    if "temp" in t: return "°C"
    if "co2" in t: return "ppm"
    return ""

def get_db_connection():
    """Crée une connexion SQLite (utilitaire pour éviter de répéter le code)."""
    if not os.path.exists(DB_FILE):
        raise FileNotFoundError(f"Base de données introuvable : {DB_FILE}")
    return sqlite3.connect(DB_FILE)

# ======================================================================
# ETL & MIGRATION (Logique Optimisée)
# ======================================================================

def extract_data_optimized(conn: sqlite3.Connection) -> Dict[str, pd.DataFrame]:
    """
    Charge les données depuis SQLite. 
    OPTI : On fait des jointures SQL ici plutôt que des merges Pandas plus tard.
    """
    data = {}
    
    # 1. Chargement Lignes avec calculs pré-faits si possible
    # SELECT * FROM Ligne
    data["lignes"] = pd.read_sql_query("SELECT * FROM Ligne", conn)
    
    # 2. Chargement Arrêts + Horaires (Jointure SQL)
    # On récupère tout d'un coup pour éviter les boucles
    query_arrets_horaires = """
    SELECT 
        a.id_arret, a.nom as nom_arret, a.latitude, a.longitude, a.id_ligne,
        h.heure_prevue, h.passagers_estimes,
        v.id_vehicule, v.immatriculation, v.type_vehicule
    FROM Arret a
    LEFT JOIN Horaire h ON a.id_arret = h.id_arret
    LEFT JOIN Vehicule v ON h.id_vehicule = v.id_vehicule
    """
    data["arrets_complets"] = pd.read_sql_query(query_arrets_horaires, conn)

    # 3. Chargement Trafic + Incidents
    query_trafic = """
    SELECT t.*, i.description, i.gravite
    FROM Trafic t
    LEFT JOIN Incident i ON t.id_trafic = i.id_trafic
    """
    data["trafic_complet"] = pd.read_sql_query(query_trafic, conn)
    
    # 4. Chargement Véhicules + Chauffeurs (Cache)
    query_vehicules = """
    SELECT v.*, c.nom as nom_chauffeur 
    FROM Vehicule v
    JOIN Chauffeur c ON v.id_chauffeur = c.id_chauffeur
    """
    data["vehicules_complets"] = pd.read_sql_query(query_vehicules, conn)
    
    # 5. Capteurs + Mesures (Attention au volume)
    # On charge les mesures jointes aux capteurs
    query_capteurs = """
    SELECT c.*, m.valeur, m.horodatage, m.unite
    FROM Capteur c
    JOIN Mesure m ON c.id_capteur = m.id_capteur
    """
    data["capteurs_mesures"] = pd.read_sql_query(query_capteurs, conn)
    
    # 6. Quartiers
    data["quartiers"] = pd.read_sql_query("SELECT * FROM Quartier", conn)
    data["arret_quartier"] = pd.read_sql_query("SELECT * FROM ArretQuartier", conn)

    return data

def transform_lignes(data: Dict[str, pd.DataFrame]) -> List[Dict]:
    """Transforme les DataFrames relationnels en documents 'lignes'."""
    df_lignes = data["lignes"]
    df_arrets = data["arrets_complets"]
    df_trafic = data["trafic_complet"]
    df_vehicules = data["vehicules_complets"]
    
    documents = []
    
    # Pour optimiser, on groupe les dataframes annexes par id_ligne une seule fois
    arrets_grp = df_arrets.groupby("id_ligne") if not df_arrets.empty else None
    trafic_grp = df_trafic.groupby("id_ligne") if not df_trafic.empty else None
    vehic_grp = df_vehicules.groupby("id_ligne") if not df_vehicules.empty else None

    for _, row in df_lignes.iterrows():
        id_ligne = row["id_ligne"]
        doc = {
            "id_ligne": int(id_ligne),
            "nom_ligne": row["nom_ligne"],
            "type": row["type"],
            "frequentation_moyenne": row.get("frequentation_moyenne"),
        }

        # Construction des sous-documents via les groupes Pandas (rapide)
        if arrets_grp and id_ligne in arrets_grp.groups:
            # Logique simplifiée : conversion du groupe en liste de dicts
            # (Dans la réalité, tu formates ici ta structure imbriquée complexe)
            groupe = arrets_grp.get_group(id_ligne)
            # Simplification pour l'exemple : on prend les arrêts uniques
            doc["arrets"] = groupe[["id_arret", "nom_arret"]].drop_duplicates().to_dict("records")
        
        if vehic_grp and id_ligne in vehic_grp.groups:
            # Cache véhicules
            doc["vehicules_cache"] = vehic_grp.get_group(id_ligne)[["id_vehicule", "immatriculation", "type_vehicule"]].to_dict("records")
            
        if trafic_grp and id_ligne in trafic_grp.groups:
            t_data = trafic_grp.get_group(id_ligne)
            # Calcul stats trafic
            doc["stats_trafic"] = {
                "total_retard": float(t_data["retard_minutes"].sum()),
                "nb_trajets": int(len(t_data))
            }
            doc["trafic"] = t_data[["id_trafic", "retard_minutes"]].to_dict("records")

        documents.append(doc)
    return documents

def transform_quartiers(data: Dict[str, pd.DataFrame]) -> List[Dict]:
    """Construit la collection quartiers."""
    df_q = data["quartiers"]
    # Logique similaire : construction des documents
    docs = []
    for _, row in df_q.iterrows():
        d = {
            "id_quartier": int(row["id_quartier"]),
            "nom": row["nom"],
            "geom": parse_geojson_geometry(row.get("geojson"))
        }
        docs.append(d)
    return docs

def transform_capteurs(data: Dict[str, pd.DataFrame]) -> List[Dict]:
    """Construit la collection capteurs (orientée mesures)."""
    df_cm = data["capteurs_mesures"]
    if df_cm.empty: return []

    docs = []
    # On groupe par capteur pour créer le document
    for id_capteur, group in df_cm.groupby("id_capteur"):
        first = group.iloc[0]
        d = {
            "id_capteur": int(id_capteur),
            "type_capteur": first["type_capteur"],
            "position": {"type": "Point", "coordinates": [first.get("longitude", 0), first.get("latitude", 0)]},
            # On embarque toutes les mesures
            "mesures": group[["valeur", "horodatage", "unite"]].to_dict("records")
        }
        docs.append(d)
    return docs

def migrer_base_de_donnees():
    """
    Orchestre la migration complète.
    Utilise st.status pour le feedback visuel.
    """
    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    
    # 1. Nettoyage
    st.write("🧹 Nettoyage de la base MongoDB...")
    for col in ["lignes", "quartiers", "capteurs"]:
        db[col].drop()
    
    # 2. Connexion SQL
    conn = get_db_connection()
    
    # 3. Extraction (SQL Optimisé)
    st.write("📥 Extraction des données SQLite (Jointures)...")
    raw_data = extract_data_optimized(conn)
    conn.close()
    
    # 4. Transformation & Chargement (Lignes)
    st.write("⚙️ Transformation et insertion : Lignes...")
    docs_lignes = transform_lignes(raw_data)
    if docs_lignes:
        db.lignes.insert_many(docs_lignes)
        db.lignes.create_index("id_ligne")
    
    # 5. Transformation & Chargement (Quartiers)
    st.write("⚙️ Transformation et insertion : Quartiers...")
    docs_quartiers = transform_quartiers(raw_data)
    if docs_quartiers:
        db.quartiers.insert_many(docs_quartiers)
        db.quartiers.create_index([("geom", "2dsphere")])

    # 6. Transformation & Chargement (Capteurs)
    st.write("⚙️ Transformation et insertion : Capteurs...")
    docs_capteurs = transform_capteurs(raw_data)
    if docs_capteurs:
        db.capteurs.insert_many(docs_capteurs)
    
    client.close()

# ======================================================================
# REQUÊTES MÉTIER (CACHÉES)
# ======================================================================

# Dictionnaire des objectifs (Texte)
REQUETES_OBJECTIFS = {
    "A": "Calculer la moyenne des retards (en minutes) pour chaque ligne...",
    "B": "Estimer le nombre moyen de passagers...",
    "C": "Calculer le taux d'incidents...",
    # ... (Ajouter les autres descriptions D à N ici)
    "N": "Classifier les lignes par catégorie de fréquentation..."
}

@st.cache_data(show_spinner=False)
def get_sql_results() -> Dict[str, pd.DataFrame]:
    """Exécute toutes les requêtes SQL et met en cache le résultat."""
    conn = get_db_connection()
    results = {}
    
    # --- REQUÊTES SQL ---
    
    # Requête A
    # mettre la requete (A) ici
    query_a = "SELECT 'Exemple' as col1" # Placeholder
    results["A"] = pd.read_sql_query(query_a, conn)

    # Requête B
    # mettre la requete (B) ici
    query_b = "SELECT 'Exemple' as col1" # Placeholder
    results["B"] = pd.read_sql_query(query_b, conn)
    
    # ... (Ajouter C à N ici avec le même pattern) ...

    conn.close()
    return results

@st.cache_data(show_spinner=False)
def get_mongo_results() -> Dict[str, pd.DataFrame]:
    """Exécute toutes les agrégations MongoDB et met en cache."""
    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    results = {}

    try:
        # --- REQUÊTES MONGO ---

        # Requête A
        # mettre la requete (A) pipeline ici
        pipeline_a = [] # Placeholder
        results["A"] = pd.DataFrame(list(db.lignes.aggregate(pipeline_a)))

        # Requête B
        # mettre la requete (B) pipeline ici
        pipeline_b = [] # Placeholder
        results["B"] = pd.DataFrame(list(db.lignes.aggregate(pipeline_b)))

        # ... (Ajouter C à N ici) ...
        
    except Exception as e:
        # En cas d'erreur de connexion ou autre
        return {"Erreur": pd.DataFrame([{"error": str(e)}])}
    finally:
        client.close()
    
    return results

def comparer_dataframes(df1, df2):
    """Logique simple de comparaison pour la partie 5."""
    if df1 is None or df2 is None or df1.empty or df2.empty:
        return "⚠️", "Données manquantes"
    if len(df1) == len(df2):
        return "✅", "Tailles identiques"
    return "❌", f"Différence : {len(df1)} vs {len(df2)}"

# ======================================================================
# FONCTIONS UI (PARTIES)
# ======================================================================

def render_partie_1_sqlite():
    st.subheader("Partie 1 : Requêtes SQLite")
    
    if st.button("Exécuter Requêtes SQL"):
        with st.spinner("Exécution..."):
            res = get_sql_results()
            st.session_state["resultats_sql"] = res
            st.success("Terminé !")

    if "resultats_sql" in st.session_state:
        res = st.session_state["resultats_sql"]
        for code, df in res.items():
            with st.expander(f"Requête {code}"):
                st.dataframe(df)

def render_partie_2_migration():
    st.subheader("Partie 2 : Migration vers MongoDB")
    
    if st.button("Lancer Migration (ETL Optimisé)", type="primary"):
        # Utilisation de st.status pour un feedback propre et natif
        with st.status("Migration en cours...", expanded=True) as status:
            try:
                migrer_base_de_donnees()
                # On vide le cache Mongo car les données ont changé
                get_mongo_results.clear()
                status.update(label="Migration terminée avec succès ! 🎉", state="complete", expanded=False)
                st.balloons()
            except Exception as e:
                status.update(label="Erreur durant la migration", state="error")
                st.error(f"Détail de l'erreur : {str(e)}")

def render_partie_3_mongo():
    st.subheader("Partie 3 : Requêtes MongoDB")
    
    if st.button("Exécuter Requêtes MongoDB"):
        with st.spinner("Exécution..."):
            res = get_mongo_results()
            st.session_state["resultats_mongo"] = res
            st.success("Terminé !")

    if "resultats_mongo" in st.session_state:
        res = st.session_state["resultats_mongo"]
        for code, df in res.items():
            with st.expander(f"Requête {code}"):
                st.dataframe(df)

def render_partie_5_comparaison():
    st.subheader("Partie 5 : Validation")
    
    res_sql = st.session_state.get("resultats_sql")
    res_mongo = st.session_state.get("resultats_mongo")

    if not res_sql or not res_mongo:
        st.warning("Veuillez d'abord exécuter les parties 1 et 3.")
        return

    for code in REQUETES_OBJECTIFS.keys():
        df_s = res_sql.get(code)
        df_m = res_mongo.get(code)
        icon, msg = comparer_dataframes(df_s, df_m)
        
        with st.expander(f"{icon} Requête {code} : {REQUETES_OBJECTIFS[code]}"):
            st.info(msg)
            c1, c2 = st.columns(2)
            c1.dataframe(df_s)
            c2.dataframe(df_m)

def render_partie_6_ia():
    st.subheader("Partie 6 : Assistant IA")
    api_key = st.session_state.get("groq_api_key")
    
    if not api_key:
        st.error("Veuillez configurer votre clé API Groq dans la sidebar.")
        return

    q = st.text_area("Votre question sur les données :")
    if st.button("Générer Pipeline"):
        client = Groq(api_key=api_key)
        # Appel API simplifié pour l'exemple
        try:
            completion = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": SCHEMA_CONTEXT + " Réponds en JSON uniquement."},
                    {"role": "user", "content": q}
                ],
                response_format={"type": "json_object"}
            )
            resp = json.loads(completion.choices[0].message.content)
            st.json(resp)
            # Exécution réelle optionnelle ici...
        except Exception as e:
            st.error(f"Erreur IA : {e}")

# ======================================================================
# MAIN APP
# ======================================================================

def main():
    st.set_page_config(page_title="Paris 2055", layout="wide")
    
    # Initialisation Session State basique
    if "groq_api_key" not in st.session_state:
        st.session_state["groq_api_key"] = os.getenv("GROQ_API_KEY", "")

    # --- SIDEBAR ---
    with st.sidebar:
        st.header("Config & Maintenance")
        
        # Clé API
        key = st.text_input("Groq API Key", value=st.session_state["groq_api_key"], type="password")
        if key != st.session_state["groq_api_key"]:
            st.session_state["groq_api_key"] = key
            # Petite astuce pour sauvegarder sans planter si pas de .env
            try: set_key(".env", "GROQ_API_KEY", key) 
            except: pass

        st.divider()
        
        # Bouton Reset Global
        if st.button("🗑️ DROP DB & RESET CACHE", type="primary"):
            # 1. Drop Mongo
            try:
                c = pymongo.MongoClient(MONGO_URI)
                c.drop_database(MONGO_DB_NAME)
                c.close()
            except: pass
            
            # 2. Clear Streamlit Cache (Magie !)
            st.cache_data.clear()
            
            # 3. Clear Session
            for k in ["resultats_sql", "resultats_mongo"]:
                if k in st.session_state: del st.session_state[k]
            
            st.toast("Système réinitialisé !", icon="💥")
            time.sleep(1)
            st.rerun()

    # --- TABS ---
    t1, t2, t3, t4, t5, t6 = st.tabs(["SQL", "Migration", "Mongo", "Dashboard", "Comparaison", "IA"])
    
    with t1: render_partie_1_sqlite()
    with t2: render_partie_2_migration()
    with t3: render_partie_3_mongo()
    with t4: st.info("Place pour graphiques")
    with t5: render_partie_5_comparaison()
    with t6: render_partie_6_ia()

if __name__ == "__main__":
    main()