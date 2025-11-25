################ Lancer l'application avec : ###########################
# streamlit run app.py
########################################################################
import os
import sqlite3
import time
import json
from typing import Callable, Dict, List, Optional

import pandas as pd
import pymongo
from pymongo.errors import PyMongoError
import streamlit as st
from groq import Groq
from dotenv import load_dotenv, set_key


DOSSIER_DATA = "data"
DOSSIER_SQLITE = "sqlite"
DB_FILE = os.path.join(DOSSIER_DATA, DOSSIER_SQLITE, "db", "paris2055.sqlite")
DOSSIER_CSV = "./data/sqlite/resultats_requetes_sqlite/"
DOSSIER_JSON = os.path.join(DOSSIER_DATA, "mongodb", "collections")
DOSSIER_MONGO_CSV = os.path.join(
    DOSSIER_DATA,
    "mongodb",
    "resultats_requetes_mongodb",
)

MONGO_URI = "mongodb://127.0.0.1:27017/"
MONGO_DB_NAME = "Paris2055"

load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")

SCHEMA_CONTEXT = """
Tu es un expert MongoDB et Python. Ton but est de traduire une question naturelle en requête d'agrégation MongoDB.
Voici le schéma de la base de données 'Paris2055' :

1. Collection 'lignes' :
    - Documents : { "id_ligne": int, "nom_ligne": str, "type": str (Bus, Tramway...), "frequentation_moyenne": int, "arrets": [...], "trafic": [{ "retard_minutes": int, "incidents": [...] }] }

2. Collection 'capteurs' :
    - Documents : { "id_capteur": int, "type_capteur": str (Bruit, CO2, Temperature), "mesures": [{ "valeur": float, "horodatage": date }], "arret": { "nom_ligne": str, "nom_arret": str } }

3. Collection 'quartiers' :
    - Documents : { "nom": str, "arrets": [...] }

RÈGLES STRICTES DE GÉNÉRATION :
1. Tu dois répondre UNIQUEMENT un objet JSON valide au format :
    {
      "collection": "nom_collection",
      "pipeline": [ ... ]
    }

2. **RÈGLE D'AFFICHAGE (PROJECTION)** : 
    Tu dois OBLIGATOIREMENT ajouter une étape `"$project"` à la fin du pipeline pour nettoyer le résultat.
    - Garde UNIQUEMENT le nom de l'entité (ex: `nom_ligne`, `nom`, ou `arret.nom_ligne`) et la valeur calculée/demandée.
    - SUPPRIME systématiquement `_id` (`"_id": 0`).
    - SUPPRIME systématiquement les listes lourdes (`arrets`, `trafic`, `mesures`) sauf si l'utilisateur demande explicitement de les voir.

3. **RÈGLE D'IMPOSSIBILITÉ** : 
    Si la question de l'utilisateur n'a ABSOLUMENT rien à voir avec le schéma de la base de données, ou s'il est techniquement impossible d'y répondre avec une requête MongoDB (par exemple, une question de philosophie ou une requête impossible même avec l'agrégation), tu dois retourner le JSON suivant, sans changer la collection :
    {
      "collection": "lignes",
      "pipeline": []
    }
"""

MIGRATION_LOG_PLACEHOLDER: Optional[st.delta_generator.DeltaGenerator] = None
MAX_LOG_LINES = 300


# =====================================================================
# UTILITAIRES GENERAUX
# =====================================================================

def enregistrer_resultats_csv(
    lien_dossier: str,
    nom_fichier: str,
    dataframe: pd.DataFrame,
) -> None:
    """
    Enregistre un DataFrame au format CSV dans le dossier indiqué.

    Paramètres
    ----------
    lien_dossier : str
        Chemin du dossier de sortie.
    nom_fichier : str
        Nom du fichier CSV à créer.
    dataframe : pandas.DataFrame
        Données à exporter.

    Retour
    ------
    None
        Le fichier est écrit sur le disque, une exception est levée en cas d'erreur.
    """
    os.makedirs(lien_dossier, exist_ok=True)
    full_path = os.path.join(lien_dossier, nom_fichier)
    dataframe.to_csv(full_path, index=False, encoding="utf-8-sig")


def aggregate_to_df(collection, pipeline: List[Dict]) -> pd.DataFrame:
    """
    Exécute un pipeline d'agrégation MongoDB et retourne le résultat en DataFrame.

    Paramètres
    ----------
    collection :
        Collection MongoDB sur laquelle exécuter l'agrégation.
    pipeline : list[dict]
        Pipeline d'agrégation MongoDB.

    Retour
    ------
    pandas.DataFrame
        Résultat de l'agrégation, vide si aucun document n'est retourné.
    """
    documents = list(collection.aggregate(pipeline))
    if not documents:
        return pd.DataFrame()
    return pd.DataFrame(documents)


def to_datetime(value) -> Optional[object]:
    """
    Convertit une valeur vers un objet datetime Python si possible.

    Paramètres
    ----------
    value :
        Valeur initiale (texte ou datetime-like).

    Retour
    ------
    datetime | None
        Objet datetime si la conversion réussit, None sinon.
    """
    if pd.isna(value):
        return None
    try:
        dt = pd.to_datetime(value, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.to_pydatetime()
    except Exception:
        return None


def parse_geojson_geometry(geojson_str: Optional[str]) -> Optional[Dict]:
    """
    Extrait la géométrie (type / coordinates) d'une chaîne représentant un objet GeoJSON.

    Paramètres
    ----------
    geojson_str : str | None
        Chaîne JSON représentant un objet GeoJSON ou une Feature.

    Retour
    ------
    dict | None
        Dictionnaire avec les clés 'type' et 'coordinates', ou None si non exploitable.
    """
    if not isinstance(geojson_str, str):
        return None

    data = geojson_str.strip()
    if not data:
        return None

    try:
        obj = json.loads(data)
    except Exception:
        return None

    # Cas simple : la chaîne représente directement une géométrie GeoJSON.
    if isinstance(obj, dict):
        if "type" in obj and "coordinates" in obj:
            return {"type": obj["type"], "coordinates": obj["coordinates"]}

        # Cas Feature GeoJSON : on récupère la sous-clé geometry.
        geometry = obj.get("geometry")
        if isinstance(geometry, dict) and {
            "type",
            "coordinates",
        }.issubset(geometry.keys()):
            return {
                "type": geometry["type"],
                "coordinates": geometry["coordinates"],
            }

    return None


def infer_unite_from_type(type_capteur: Optional[str]) -> Optional[str]:
    """
    Déduit une unité de mesure par défaut selon le type de capteur.

    Paramètres
    ----------
    type_capteur : str | None
        Libellé du type de capteur (Bruit, CO2, Température, etc.).

    Retour
    ------
    str | None
        Unité probable (dB, °C, ppm), ou None si aucune correspondance évidente.
    """
    if not isinstance(type_capteur, str):
        return None

    lower_value = type_capteur.lower()
    if "bruit" in lower_value:
        return "dB"
    if "temp" in lower_value:
        return "°C"
    if "co2" in lower_value:
        return "ppm"
    return None


# =====================================================================
# CONNECTIVITE MONGODB ET LOGS
# =====================================================================

def check_connexion_details() -> tuple[bool, bool]:
    """
    Vérifie l'état du serveur MongoDB et la présence de la base Paris2055.

    Retour
    ------
    (bool, bool)
        - premier booléen : True si le serveur répond au ping.
        - second booléen : True si la base MONGO_DB_NAME existe sur le serveur.
    """
    client: Optional[pymongo.MongoClient] = None
    server_ok = False
    db_ok = False

    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        client.admin.command("ping")
        server_ok = True

        if MONGO_DB_NAME in client.list_database_names():
            db_ok = True
    except Exception:
        # Le détail de l'exception est traité plus haut dans l'IHM.
        server_ok = False
        db_ok = False
    finally:
        if client is not None:
            client.close()

    return server_ok, db_ok


def log_progress(
    current: int,
    total: int,
    prefix: str,
    log_fn: Callable[[str, bool], None],
    step_percent: Optional[int] = None,
) -> None:
    """
    Trace la progression d'un traitement long dans les logs.

    Paramètres
    ----------
    current : int
        Position actuelle dans le traitement.
    total : int
        Nombre total d'éléments à traiter.
    prefix : str
        Libellé du traitement (affiché dans le message).
    log_fn : Callable[[str, bool], None]
        Fonction utilisée pour écrire dans le journal (CLI ou Streamlit).
    step_percent : int | None
        Fréquence de rafraîchissement de l'affichage en pourcentage (10 -> tous les 10%).

    Retour
    ------
    None
    """
    if total == 0:
        return

    if step_percent:
        step = max(int(total * (step_percent / 100)), 1)
        # Mise à jour limitée pour ne pas saturer les logs.
        if current % step != 0 and current != total:
            return

    pct = (current / total) * 100
    message = f"    >> {prefix} : {current:,} / {total:,} ({pct:.1f}%)"
    log_fn(message, replace_last=True)


# =====================================================================
# TRANSFORMATION DATAFRAME -> DOCUMENTS + SAUVEGARDE JSON / MONGO
# =====================================================================

def dataframe_to_dict_progressive(
    df: pd.DataFrame,
    label: str,
    log_fn: Callable[[str, bool], None],
    batch_size: int = 1000,
) -> List[Dict]:
    """
    Convertit un DataFrame en liste de dictionnaires avec journalisation progressive.

    Paramètres
    ----------
    df : pandas.DataFrame
        Données en entrée.
    label : str
        Libellé du traitement pour les messages de progression.
    log_fn : Callable[[str, bool], None]
        Fonction de log utilisée pour afficher la progression.
    batch_size : int
        Taille des paquets traités à chaque itération.

    Retour
    ------
    list[dict]
        Liste complète de documents prêts à être insérés en base.
    """
    total = len(df)
    documents: List[Dict] = []

    log_progress(0, total, label, log_fn)

    # Traitement par paquets pour limiter la consommation mémoire et
    # éviter d'inonder l'interface de logs.
    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        batch = df.iloc[start:end].to_dict("records")
        documents.extend(batch)
        log_progress(len(documents), total, label, log_fn)

    return documents


def sauvegarder_collection_json(
    nom_collection: str,
    data: List[Dict],
    log_fn: Callable[[str, bool], None],
) -> str:
    """
    Sauvegarde une collection métier dans un fichier JSON unique.

    Paramètres
    ----------
    nom_collection : str
        Nom fonctionnel de la collection (utilisé pour le fichier).
    data : list[dict]
        Liste des documents à sérialiser.
    log_fn : Callable[[str, bool], None]
        Fonction de log pour suivre l'avancement.

    Retour
    ------
    str
        Nom du fichier JSON écrit sur le disque.
    """
    file_name = f"Collection_{nom_collection}.json"
    full_path = os.path.join(DOSSIER_JSON, file_name)
    total = len(data)

    log_fn(
        f"   💾 [JSON] Sauvegarde sur disque : {file_name} ({total:,} documents)...",
        replace_last=False,
    )

    try:
        with open(full_path, "w", encoding="utf-8") as json_file:
            # default=str permet de sérialiser les types non natifs (datetime, Timestamp...).
            json.dump(data, json_file, ensure_ascii=False, default=str)

        log_fn(
            f"   ✅ [JSON] Fichier écrit : {total:,} documents.",
            replace_last=False,
        )
        return file_name
    except Exception as exc:
        log_fn(f"   💥 [ERREUR] JSON : {exc}", replace_last=False)
        raise


def insert_with_progress(
    collection,
    docs: List[Dict],
    label: str,
    batch_size: int = 25000,
    log_fn: Callable[[str, bool], None] = print,
) -> None:
    """
    Insère une liste de documents dans une collection MongoDB par gros paquets.

    Paramètres
    ----------
    collection :
        Collection MongoDB cible.
    docs : list[dict]
        Documents à insérer.
    label : str
        Libellé fonctionnel de la collection (pour les logs).
    batch_size : int
        Nombre de documents insérés par batch.
    log_fn : Callable[[str, bool], None]
        Fonction de log pour suivre la progression.

    Retour
    ------
    None
    """
    total = len(docs)
    if total == 0:
        return

    log_fn(
        f"   📤 [MONGO] Injection de {total:,} documents dans '{label}'...",
        replace_last=False,
    )
    log_progress(0, total, "Insertion MongoDB", log_fn)

    inserted_count = 0

    # Insertion par paquets pour éviter les demandes trop volumineuses
    # et laisser MongoDB optimiser l'ordre des insertions.
    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        batch = docs[start:end]
        collection.insert_many(batch, ordered=False)
        inserted_count += len(batch)
        log_progress(
            inserted_count,
            total,
            "Insertion MongoDB",
            log_fn,
            step_percent=10,
        )

    log_fn(
        f"   ✨ [OK] Collection '{label}' entièrement migrée.\n",
        replace_last=False,
    )


# =====================================================================
# CHARGEMENT DES TABLES SQLITE ET CONSTRUCTION DES DOCUMENTS METIER
# =====================================================================

def load_tables(
    conn: sqlite3.Connection,
    log_fn: Callable[[str, bool], None],
) -> Dict[str, pd.DataFrame]:
    """
    Charge les tables nécessaires depuis la base SQLite dans un dictionnaire de DataFrame.

    Paramètres
    ----------
    conn : sqlite3.Connection
        Connexion SQLite déjà ouverte.
    log_fn : Callable[[str, bool], None]
        Fonction de log pour tracer les étapes de chargement.

    Retour
    ------
    dict[str, pandas.DataFrame]
        Dictionnaire mappant le nom de la table vers son contenu.
    """
    log_fn("📥 [SQL] Lecture des tables SQLite source...", replace_last=False)
    tables: Dict[str, pd.DataFrame] = {}
    table_names = [
        "Ligne",
        "Quartier",
        "Arret",
        "ArretQuartier",
        "Chauffeur",
        "Vehicule",
        "Horaire",
        "Capteur",
        "Mesure",
        "Trafic",
        "Incident",
    ]

    for table_name in table_names:
        tables[table_name] = pd.read_sql_query(
            f"SELECT * FROM {table_name}",
            conn,
        )

    log_fn(
        f"   🧱 {len(tables)} tables chargées en mémoire.",
        replace_last=False,
    )
    return tables

def build_lignes_docs(
    tables: Dict[str, pd.DataFrame],
    log_fn: Callable[[str, bool], None],
) -> List[Dict]:
    """
    Construit les documents 'lignes' avec une optimisation majeure (Dénormalisation)
    pour rendre les requêtes D et K instantanées.
    """
    df_l = tables["Ligne"]
    df_a = tables["Arret"]
    df_aq = tables["ArretQuartier"]
    df_q = tables["Quartier"]
    df_h = tables["Horaire"]
    df_v = tables["Vehicule"]
    df_c = tables["Chauffeur"]
    df_t = tables["Trafic"]
    df_i = tables["Incident"]
    df_cap = tables["Capteur"]
    df_mes = tables["Mesure"]

    # 1. Pré-calcul CO2 (inchangé)
    co2_by_ligne: Dict[int, float] = {}
    if not df_cap.empty and not df_mes.empty:
        df_full = df_cap.merge(df_mes, on="id_capteur")
        df_co2 = df_full[df_full["type_capteur"] == "CO2"]
        df_co2_ligne = df_co2.merge(df_a[["id_arret", "id_ligne"]], on="id_arret")
        if not df_co2_ligne.empty:
            co2_by_ligne = df_co2_ligne.groupby("id_ligne")["valeur"].mean().to_dict()

    # 2. OPTIMISATION QUERY K : Cache Chauffeurs
    log_fn("⚡ [OPTIM] Construction du cache Chauffeurs...", replace_last=False)
    chauffeurs_cache_by_ligne: Dict[int, List[Dict]] = {}
    if not df_v.empty and not df_c.empty:
        df_vc = df_v.merge(df_c, on="id_chauffeur", how="inner")
        if "id_ligne" in df_vc.columns:
            for id_ligne, group in df_vc.groupby("id_ligne"):
                if pd.isna(id_ligne): continue
                c_list = []
                for _, row in group.iterrows():
                    if pd.notnull(row.get("nom")):
                        c_list.append({
                            "id_chauffeur": int(row["id_chauffeur"]),
                            "nom_chauffeur": row["nom"]
                        })
                if c_list:
                    chauffeurs_cache_by_ligne[int(id_ligne)] = c_list

    # 3. OPTIMISATION QUERY D : Cache Véhicules Uniques par Ligne
    # On évite l'unwind monstrueux des horaires
    log_fn("⚡ [OPTIM] Construction du cache Véhicules...", replace_last=False)
    vehicules_cache_by_ligne: Dict[int, List[Dict]] = {}
    if not df_v.empty:
        # On suppose que df_v a une colonne id_ligne (la ligne officielle du véhicule)
        for id_ligne, group in df_v.groupby("id_ligne"):
            if pd.isna(id_ligne): continue
            v_list = []
            for _, row in group.iterrows():
                if pd.notnull(row.get("immatriculation")):
                    v_list.append({
                        "id_vehicule": int(row["id_vehicule"]),
                        "immatriculation": row["immatriculation"]
                    })
            if v_list:
                vehicules_cache_by_ligne[int(id_ligne)] = v_list

    # 4. OPTIMISATION QUERY K & A : Stats Trafic pré-calculées
    # On évite de scanner le tableau trafic en lecture
    log_fn("⚡ [OPTIM] Pré-calcul des stats Trafic...", replace_last=False)
    stats_trafic_by_ligne: Dict[int, Dict] = {}
    trafic_by_ligne: Dict[int, List[Dict]] = {}
    
    # On prépare aussi le détail pour les autres requêtes, mais on calcule les stats ici
    incidents_by_trafic = {} 
    if not df_i.empty:
        for id_trafic, group in df_i.groupby("id_trafic"):
            incidents_by_trafic[id_trafic] = group[["id_incident", "description", "gravite"]].to_dict("records")

    if not df_t.empty:
        # Calcul vectoriel Pandas (beaucoup plus rapide que Mongo)
        stats_group = df_t.groupby("id_ligne")["retard_minutes"].agg(['sum', 'count', 'mean'])
        
        for id_ligne, row_stat in stats_group.iterrows():
            stats_trafic_by_ligne[int(id_ligne)] = {
                "total_retard": float(row_stat['sum']),
                "nb_trajets": int(row_stat['count']),
                "moyenne_precalc": float(row_stat['mean'])
            }

        # Construction standard du détail trafic
        for _, row in df_t.iterrows():
            if pd.isna(row["id_ligne"]): continue
            tdoc = {"id_trafic": int(row["id_trafic"])}
            if pd.notnull(row.get("retard_minutes")): tdoc["retard_minutes"] = int(row["retard_minutes"])
            if row["id_trafic"] in incidents_by_trafic: tdoc["incidents"] = incidents_by_trafic[row["id_trafic"]]
            trafic_by_ligne.setdefault(int(row["id_ligne"]), []).append(tdoc)

    # 5. Préparations standards (Arrêts, Quartiers, Horaires...)
    # (Code inchangé pour la structure Arrets/Horaires, on le garde pour les requêtes B, L, etc.)
    quartiers_by_arret: Dict[int, List[Dict]] = {}
    if not df_aq.empty:
        tmp = df_aq.merge(df_q[["id_quartier", "nom"]].rename(columns={"nom": "nom_quartier"}), on="id_quartier", how="left")
        for id_arret, group in tmp.groupby("id_arret"):
            subset = group[["id_quartier", "nom_quartier"]].drop_duplicates("id_quartier")
            quartiers_by_arret[id_arret] = [
                {"id_quartier": int(row["id_quartier"]), "nom": row["nom_quartier"]}
                for _, row in subset.iterrows() if pd.notnull(row["id_quartier"])
            ]

    capteurs_ids_by_arret: Dict[int, List[int]] = {}
    if not df_cap.empty:
        for id_arret, group in df_cap.groupby("id_arret"):
            capteurs_ids_by_arret[id_arret] = [int(v) for v in group["id_capteur"].dropna().unique().tolist()]

    horaires_by_arret: Dict[int, List[Dict]] = {}
    if not df_h.empty:
        df_v_clean = df_v.rename(columns={"id_ligne": "id_ligne_officielle"})
        df_h_full = df_h.merge(df_v_clean, on="id_vehicule", how="left", suffixes=("", "_vehicule"))
        for col in ["heure_prevue"]:
             if col in df_h_full.columns: df_h_full[col] = pd.to_datetime(df_h_full[col], errors="coerce")

        total_rows = len(df_h_full)
        log_progress(0, total_rows, "Groupement Horaires/Arrêts", log_fn)

        for idx, row in enumerate(df_h_full.itertuples(index=False), start=1):
            if pd.isna(row.id_arret): continue
            
            # Construction allégée
            vehicule = {}
            if getattr(row, "id_vehicule", None) and not pd.isna(row.id_vehicule):
                vehicule["id_vehicule"] = int(row.id_vehicule)
                if hasattr(row, "type_vehicule"): vehicule["type_vehicule"] = row.type_vehicule
                if hasattr(row, "immatriculation"): vehicule["immatriculation"] = row.immatriculation
                if hasattr(row, "id_ligne_officielle") and pd.notnull(row.id_ligne_officielle):
                    vehicule["id_ligne_officielle"] = int(row.id_ligne_officielle)

            horaire = {}
            if hasattr(row, "heure_prevue") and pd.notnull(row.heure_prevue): horaire["heure_prevue"] = row.heure_prevue.to_pydatetime()
            if hasattr(row, "passagers_estimes") and pd.notnull(row.passagers_estimes): horaire["passagers_estimes"] = int(row.passagers_estimes)
            if vehicule: horaire["vehicule"] = vehicule
            
            horaires_by_arret.setdefault(int(row.id_arret), []).append(horaire)
            if idx % 10000 == 0: log_progress(idx, total_rows, "Groupement Horaires/Arrêts", log_fn)
        log_fn("", replace_last=False)

    arrets_by_ligne: Dict[int, List[Dict]] = {}
    for _, row in df_a.iterrows():
        if pd.isna(row["id_ligne"]) or pd.isna(row["id_arret"]): continue
        id_l, id_a = int(row["id_ligne"]), int(row["id_arret"])
        adoc = {"id_arret": id_a, "nom": row["nom"]}
        if id_a in quartiers_by_arret: adoc["quartiers"] = quartiers_by_arret[id_a]
        if id_a in horaires_by_arret: adoc["horaires"] = horaires_by_arret[id_a]
        if id_a in capteurs_ids_by_arret: adoc["capteurs_ids"] = capteurs_ids_by_arret[id_a]
        arrets_by_ligne.setdefault(id_l, []).append(adoc)

    # 6. Assemblage final
    docs = []
    total = len(df_l)
    log_progress(0, total, "Assemblage Lignes", log_fn)
    for idx, (_, row) in enumerate(df_l.iterrows(), start=1):
        if pd.isna(row["id_ligne"]): continue
        id_l = int(row["id_ligne"])
        doc = {
            "id_ligne": id_l, 
            "nom_ligne": row.get("nom_ligne"),
            "type": row.get("type")
        }
        
        # Injection CO2
        if id_l in co2_by_ligne: 
            doc["co2_moyen_ligne"] = co2_by_ligne[id_l]

        # Injection Cache Chauffeurs (Pour Query K)
        if id_l in chauffeurs_cache_by_ligne:
            doc["chauffeurs_cache"] = chauffeurs_cache_by_ligne[id_l]

        # Injection Cache Véhicules (Pour Query D)
        if id_l in vehicules_cache_by_ligne:
            doc["vehicules_cache"] = vehicules_cache_by_ligne[id_l]
            
        # Injection Stats Trafic (Pour Query K et A)
        if id_l in stats_trafic_by_ligne:
            doc["stats_trafic"] = stats_trafic_by_ligne[id_l]

        if id_l in arrets_by_ligne: doc["arrets"] = arrets_by_ligne[id_l]
        if id_l in trafic_by_ligne: doc["trafic"] = trafic_by_ligne[id_l]
        if pd.notnull(row.get("frequentation_moyenne")): doc["frequentation_moyenne"] = float(row["frequentation_moyenne"])
        
        docs.append(doc)
        if idx % 100 == 0: log_progress(idx, total, "Assemblage Lignes", log_fn)

    return docs

def build_quartiers_docs(
    tables: Dict[str, pd.DataFrame],
    log_fn: Callable[[str, bool], None],
) -> List[Dict]:
    """
    Construit les documents de la collection 'quartiers' à partir des tables SQLite.

    Paramètres
    ----------
    tables : dict[str, pandas.DataFrame]
        Tables SQLite préchargées.
    log_fn : Callable[[str, bool], None]
        Fonction de log pour tracer la progression.

    Retour
    ------
    list[dict]
        Documents prêts à être insérés dans la collection 'quartiers'.
    """
    df_q = tables["Quartier"]
    df_aq = tables["ArretQuartier"]
    df_a = tables["Arret"]
    df_l = tables["Ligne"]

    arrets_by_quartier: Dict[int, List[Dict]] = {}
    if not df_aq.empty:
        log_fn(
            "Running : Groupement Arrêts par quartier...",
            replace_last=False,
        )
        df_aq_full = df_aq.merge(
            df_a[["id_arret", "nom", "id_ligne"]].rename(
                columns={"nom": "nom_arret"},
            ),
            on="id_arret",
            how="left",
        )
        df_aq_full = df_aq_full.merge(
            df_l[["id_ligne", "nom_ligne"]],
            on="id_ligne",
            how="left",
        )

        groups = df_aq_full.groupby("id_quartier")
        nb_groups = len(groups)
        log_progress(0, nb_groups, "Groupement Arrêts/Quartiers", log_fn)

        for idx, (id_quartier, group) in enumerate(groups, start=1):
            subset = group[
                [
                    "id_arret",
                    "nom_arret",
                    "id_ligne",
                    "nom_ligne",
                ]
            ].drop_duplicates("id_arret")

            stops: List[Dict] = []
            for _, row in subset.iterrows():
                if pd.isna(row["id_arret"]):
                    continue
                stop_doc: Dict[str, object] = {
                    "id_arret": int(row["id_arret"]),
                    "nom": row["nom_arret"],
                }
                if pd.notnull(row.get("id_ligne")):
                    stop_doc["id_ligne"] = int(row["id_ligne"])
                if pd.notnull(row.get("nom_ligne")):
                    stop_doc["nom_ligne"] = row["nom_ligne"]
                stops.append(stop_doc)

            arrets_by_quartier[id_quartier] = stops
            log_progress(
                idx,
                nb_groups,
                "Groupement Arrêts/Quartiers",
                log_fn,
                step_percent=10,
            )
        log_fn("", replace_last=False)

    docs: List[Dict] = []
    total_quartiers = len(df_q)
    label_final = "Construction documents quartiers"
    log_progress(0, total_quartiers, label_final, log_fn)

    for idx, (_, row) in enumerate(df_q.iterrows(), start=1):
        if pd.isna(row["id_quartier"]):
            continue

        id_quartier = int(row["id_quartier"])
        doc: Dict[str, object] = {
            "id_quartier": id_quartier,
            "nom": row.get("nom"),
        }

        geom = parse_geojson_geometry(row.get("geojson"))
        if geom is not None:
            doc["geom"] = geom

        stops = arrets_by_quartier.get(id_quartier)
        if stops:
            doc["arrets"] = stops

        docs.append(doc)
        if (idx % 100 == 0) or (idx == total_quartiers):
            log_progress(idx, total_quartiers, label_final, log_fn)

    log_fn("", replace_last=False)
    return docs


def build_capteurs_docs(
    tables: Dict[str, pd.DataFrame],
    log_fn: Callable[[str, bool], None],
) -> List[Dict]:
    """
    Construit les documents de la collection 'capteurs' à partir des tables SQLite.

    Paramètres
    ----------
    tables : dict[str, pandas.DataFrame]
        Tables SQLite préchargées.
    log_fn : Callable[[str, bool], None]
        Fonction de log pour suivre le traitement.

    Retour
    ------
    list[dict]
        Documents prêts à être insérés dans la collection 'capteurs'.
    """
    df_capteur = tables["Capteur"]
    df_mesure = tables["Mesure"]
    df_arret = tables["Arret"]
    df_ligne = tables["Ligne"]

    arret_info_by_id: Dict[int, Dict] = {}
    if not df_arret.empty:
        df_a_l = df_arret.merge(
            df_ligne[["id_ligne", "nom_ligne"]],
            on="id_ligne",
            how="left",
        )
        for _, row in df_a_l.iterrows():
            if pd.isna(row["id_arret"]):
                continue

            arret_id = int(row["id_arret"])
            info: Dict[str, object] = {
                "id_arret": arret_id,
                "nom": row.get("nom"),
            }
            if "id_ligne" in row and pd.notnull(row["id_ligne"]):
                info["id_ligne"] = int(row["id_ligne"])
            if "nom_ligne" in row and pd.notnull(row["nom_ligne"]):
                info["nom_ligne"] = row["nom_ligne"]
            arret_info_by_id[arret_id] = info

    type_capteur_by_id: Dict[int, Optional[str]] = {}
    for _, row in df_capteur.iterrows():
        if pd.isna(row["id_capteur"]):
            continue

        capteur_id = int(row["id_capteur"])
        type_capteur_by_id[capteur_id] = row.get("type_capteur")

    mesures_by_capteur: Dict[int, List[Dict]] = {}
    if not df_mesure.empty:
        log_fn(
            "Running : Groupement Mesures par capteur...",
            replace_last=False,
        )
        groups = df_mesure.groupby("id_capteur")
        nb_groups = len(groups)
        log_progress(0, nb_groups, "Groupement Mesures/Capteurs", log_fn)
        has_unite_col = "unite" in df_mesure.columns

        for idx, (capteur_id, group) in enumerate(groups, start=1):
            meas_list: List[Dict] = []
            for _, row in group.iterrows():
                meas_doc: Dict[str, object] = {}
                if "id_mesure" in row and pd.notnull(row["id_mesure"]):
                    meas_doc["id_mesure"] = int(row["id_mesure"])
                if "horodatage" in row and pd.notnull(row["horodatage"]):
                    dt_measure = pd.to_datetime(
                        row["horodatage"],
                        errors="coerce",
                    )
                    if not pd.isna(dt_measure):
                        meas_doc["horodatage"] = dt_measure.to_pydatetime()
                if "valeur" in row and pd.notnull(row["valeur"]):
                    try:
                        meas_doc["valeur"] = float(row["valeur"])
                    except Exception:
                        pass

                if has_unite_col and pd.notnull(row.get("unite")):
                    unite_val = row["unite"]
                else:
                    type_cap = type_capteur_by_id.get(int(capteur_id))
                    unite_val = infer_unite_from_type(type_cap)

                if unite_val is not None:
                    meas_doc["unite"] = unite_val

                meas_list.append(meas_doc)

            mesures_by_capteur[int(capteur_id)] = meas_list
            log_progress(
                idx,
                nb_groups,
                "Groupement Mesures/Capteurs",
                log_fn,
                step_percent=10,
            )
        log_fn("", replace_last=False)

    docs: List[Dict] = []
    total_capteurs = len(df_capteur)
    label_final = "Construction documents capteurs"
    log_progress(0, total_capteurs, label_final, log_fn)

    for idx, (_, row) in enumerate(df_capteur.iterrows(), start=1):
        if pd.isna(row["id_capteur"]):
            continue

        capteur_id = int(row["id_capteur"])
        doc: Dict[str, object] = {"id_capteur": capteur_id}

        if "type_capteur" in row and pd.notnull(row["type_capteur"]):
            doc["type_capteur"] = row["type_capteur"]

        latitude = row.get("latitude")
        longitude = row.get("longitude")
        if pd.notnull(latitude) and pd.notnull(longitude):
            doc["position"] = {
                "type": "Point",
                "coordinates": [float(longitude), float(latitude)],
            }

        if "id_arret" in row and pd.notnull(row["id_arret"]):
            arret_id = int(row["id_arret"])
            info = arret_info_by_id.get(arret_id)
            if info:
                doc["arret"] = info

        doc["mesures"] = mesures_by_capteur.get(capteur_id, [])

        docs.append(doc)
        if (idx % 500 == 0) or (idx == total_capteurs):
            log_progress(idx, total_capteurs, label_final, log_fn)

    log_fn("", replace_last=False)
    return docs


# =====================================================================
# ORCHESTRATION MIGRATION SQLITE -> MONGODB
# =====================================================================

def creer_index_mongodb(
    db: pymongo.database.Database,
    log_fn: Callable[[str, bool], None],
) -> None:
    """
    Crée les index utiles pour accélérer les principales requêtes MongoDB.

    Paramètres
    ----------
    db : pymongo.database.Database
        Base MongoDB 'Paris2055'.
    log_fn : Callable[[str, bool], None]
        Fonction de log pour tracer la création des index.

    Retour
    ------
    None
    """
    log_fn("[Index] Création des index MongoDB...", replace_last=False)
    try:
        db.lignes.create_index("id_ligne")
        db.lignes.create_index("nom_ligne")
        db.lignes.create_index("type")

        db.capteurs.create_index("id_capteur")
        db.capteurs.create_index("type_capteur")
        db.capteurs.create_index("arret.id_ligne")
        db.capteurs.create_index("arret.id_arret")

        db.quartiers.create_index([("geom", "2dsphere")])
        db.capteurs.create_index([("position", "2dsphere")])

        #            le nouveau modèle stockant les arrêts imbriqués dans 'lignes'
        #              et 'quartiers'. Cela évite de créer une collection vide.
        log_fn("[Index] Index créés avec succès.", replace_last=False)
    except Exception as exc:
        log_fn(
            f"[Index] Erreur lors de la création des index : {exc}",
            replace_last=False,
        )


def migrer_sqlite_vers_mongo(
    log_fn_raw: Callable[[str, bool], None],
) -> None:
    """
    Lance la migration complète de SQLite vers MongoDB pour le modèle document.

    Étapes
    ------
    - Nettoyage d'anciennes collections cibles.
    - Lecture des tables SQLite.
    - Construction des documents métier pour 'lignes', 'quartiers', 'capteurs'.
    - Sauvegarde en JSON intermédiaire.
    - Insertion des documents dans MongoDB.
    - Création des index.

    Paramètres
    ----------
    log_fn_raw : Callable[[str, bool], None]
        Fonction de log fournie par l'IHM (Streamlit).

    Retour
    ------
    None
    """

    def secure_log(message: str, replace_last: bool = False) -> None:
        """
        Adapte la fonction de log fournie pour supporter ou non l'argument
        'replace_last'.

        Cette fonction garantit que la migration reste fonctionnelle même
        si l'appelant ne gère pas ce paramètre optionnel.
        """
        try:
            log_fn_raw(message, replace_last=replace_last)
        except TypeError:
            print(message)

    def process_step(
        label: str,
        build_func: Callable[[Dict[str, pd.DataFrame], Callable[[str, bool], None]], List[Dict]],
        tables: Dict[str, pd.DataFrame],
    ) -> None:
        """
        Exécute une étape de migration complète pour une collection.

        Paramètres
        ----------
        label : str
            Nom de la collection MongoDB cible.
        build_func : Callable
            Fonction de construction des documents métier.
        tables : dict[str, pandas.DataFrame]
            Ensemble des tables SQLite préchargées.
        """
        secure_log(
            f"\n🔹 --- TRAITEMENT COLLECTION : {label.upper()} ---",
            replace_last=False,
        )

        secure_log(
            "   ⚙️  Construction du modèle métier...",
            replace_last=False,
        )
        documents = build_func(tables, secure_log)
        secure_log(
            f"   👌  Construction terminée : {len(documents):,} documents prêts.",
            replace_last=False,
        )

        sauvegarder_collection_json(label, documents, secure_log)

        insert_with_progress(
            db[label],
            documents,
            label,
            log_fn=secure_log,
        )

        # Libération mémoire volontaire pour les gros volumes.
        del documents

    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]

    secure_log("🚀 DÉBUT DE LA MIGRATION (ETL)", replace_last=False)
    secure_log(
        "   Mode : SQLite -> Modèle Document -> MongoDB",
        replace_last=False,
    )

    try:
        secure_log(
            "\n🧹 [INIT] Nettoyage de la base cible...",
            replace_last=False,
        )
        old_collections = [
            "Lignes",
            "Quartiers",
            "Vehicules",
            "Capteurs",
            "Trafics",
            "Arrets",
            "lignes",
            "quartiers",
            "capteurs",
        ]
        dropped_count = 0
        try:
            existing_collections = set(db.list_collection_names())
        except PyMongoError:
            existing_collections = set()

        for collection_name in old_collections:
            if collection_name in existing_collections:
                try:
                    db[collection_name].drop()
                    dropped_count += 1
                except PyMongoError:
                    # L'échec de suppression d'une collection ne doit pas bloquer
                    # l'ensemble du processus de migration.
                    continue

        secure_log(
            f"   🗑️  {dropped_count} anciennes collections supprimées.",
            replace_last=False,
        )

        if not os.path.exists(DB_FILE):
            raise FileNotFoundError(f"DB introuvable: {DB_FILE}")

        sqlite_conn = sqlite3.connect(DB_FILE)
        try:
            tables = load_tables(sqlite_conn, secure_log)
        finally:
            sqlite_conn.close()

        process_step("lignes", build_lignes_docs, tables)
        process_step("quartiers", build_quartiers_docs, tables)
        process_step("capteurs", build_capteurs_docs, tables)

        secure_log(
            "\n🔎 [INDEX] Optimisation de la base...",
            replace_last=False,
        )
        creer_index_mongodb(db, secure_log)

        secure_log(
            "\n🎉 --- MIGRATION TERMINÉE AVEC SUCCÈS ---",
            replace_last=False,
        )

    except Exception as exc:
        secure_log(f"\n💥 [CRITICAL ERROR]: {exc}", replace_last=False)
    finally:
        client.close()


# =====================================================================
# REQUETES SQL (PARTIE 1) ET CACHE CSV
# =====================================================================

REQUETES_OBJECTIFS: Dict[str, str] = {
    "A": (
        "Calculer la moyenne des retards (en minutes) pour chaque ligne de "
        "transport, triée par ordre décroissant."
    ),
    "B": (
        "Estimer le nombre moyen de passagers transportés par jour pour "
        "chaque ligne."
    ),
    "C": (
        "Calculer le taux d'incidents (en pourcentage) pour chaque ligne, "
        "basé sur le nombre de trajets ayant signalé un incident."
    ),
    "D": (
        "Identifier la moyenne d'émission de CO2 (captée aux arrêts) "
        "associée aux véhicules, triée par ordre décroissant."
    ),
    "E": (
        "Trouver les 5 quartiers ayant la moyenne de niveau de bruit (en dB) "
        "la plus élevée, basée sur les capteurs de bruit aux arrêts."
    ),
    "F": (
        "Identifier les lignes ayant eu des retards de plus de 10 minutes, "
        "mais qui n'ont pas eu d'incidents signalés (EXCEPT)."
    ),
    "G": (
        "Calculer le taux de ponctualité global (en pourcentage) de tous les "
        "trajets enregistrés (retard = 0)."
    ),
    "H": (
        "Compter le nombre d'arrêts desservis par chaque quartier, trié par "
        "ordre décroissant."
    ),
    "I": (
        "Corréler la moyenne des retards et la moyenne des émissions de CO2 "
        "pour chaque ligne (jointures externes)."
    ),
    "J": (
        "Calculer la moyenne de température (captée aux arrêts) pour chaque "
        "ligne de transport."
    ),
    "K": (
        "Calculer la moyenne des retards (en minutes) associée aux trajets "
        "effectués par chaque chauffeur."
    ),
    "L": (
        "Pour les lignes de Bus uniquement, calculer le pourcentage de "
        "véhicules électriques dans leur flotte."
    ),
    "M": (
        "Classifier les capteurs de CO2 par niveau de pollution ('Élevé', "
        "'Moyen', 'Faible') en fonction de la moyenne de leurs mesures."
    ),
    "N": (
        "Classifier les lignes par catégorie de fréquentation ('Haute', "
        "'Moyenne', 'Basse') basée sur leur fréquentation moyenne."
    ),
}


def executer_toutes_les_requetes() -> Dict[str, pd.DataFrame]:
    """
    Exécute l'ensemble des requêtes SQL définies A -> N sur la base SQLite.

    Retour
    ------
    dict[str, pandas.DataFrame]
        Dictionnaire associant le code de requête à son DataFrame résultat.
    """
    if not os.path.exists(DB_FILE):
        raise FileNotFoundError(DB_FILE)

    resultats: Dict[str, pd.DataFrame] = {}

    sql_queries: Dict[str, str] = {
        "A": (
            "SELECT L.nom_ligne, "
            "AVG(T.retard_minutes) AS moyenne_retard_minutes "
            "FROM Trafic AS T "
            "JOIN Ligne AS L ON T.id_ligne = L.id_ligne "
            "GROUP BY L.nom_ligne "
            "ORDER BY moyenne_retard_minutes DESC;"
        ),
        "B": (
            "SELECT "
            "    L.id_ligne, "
            "    AVG(T.total_passagers_jour) AS moyenne_passagers_jour "
            "FROM ( "
            "    SELECT "
            "        A.id_ligne, "
            "        DATE(H.heure_prevue) AS jour, "
            "        SUM(H.passagers_estimes) AS total_passagers_jour "
            "    FROM Horaire AS H "
            "    JOIN Arret AS A ON H.id_arret = A.id_arret "
            "    GROUP BY "
            "        A.id_ligne, "
            "        jour "
            ") AS T "
            "JOIN Ligne AS L ON T.id_ligne = L.id_ligne "
            "GROUP BY L.id_ligne "
            "ORDER BY moyenne_passagers_jour DESC;"
        ),
        "C": (
            "SELECT L.nom_ligne, "
            "COUNT(DISTINCT I.id_trafic) AS nb_trafic_avec_incident, "
            "COUNT(DISTINCT T.id_trafic) AS nb_total_trafic, "
            "CASE "
            "WHEN COUNT(DISTINCT T.id_trafic) = 0 THEN 0 "
            "ELSE "
            "(CAST(COUNT(DISTINCT I.id_trafic) AS REAL) "
            "/ COUNT(DISTINCT T.id_trafic)) * 100 "
            "END AS taux_incident_pourcent "
            "FROM Ligne AS L "
            "LEFT JOIN Trafic AS T ON L.id_ligne = T.id_ligne "
            "LEFT JOIN Incident AS I ON T.id_trafic = I.id_trafic "
            "GROUP BY L.nom_ligne "
            "ORDER BY taux_incident_pourcent DESC;"
        ),
        "D": (
            "SELECT V.id_vehicule, V.immatriculation, "
            "AVG(M.valeur) AS moyenne_co2 "
            "FROM Vehicule AS V "
            "JOIN Ligne AS L ON V.id_ligne = L.id_ligne "
            "JOIN Arret AS A ON L.id_ligne = A.id_ligne "
            "JOIN Capteur AS C ON A.id_arret = C.id_arret "
            "JOIN Mesure AS M ON C.id_capteur = M.id_capteur "
            "WHERE C.type_capteur = 'CO2' "
            "GROUP BY V.id_vehicule, V.immatriculation "
            "ORDER BY moyenne_co2 DESC;"
        ),
        "E": (
            "SELECT Q.nom, "
            "AVG(M.valeur) AS moyenne_bruit_db "
            "FROM Quartier AS Q "
            "JOIN ArretQuartier AS AQ ON Q.id_quartier = AQ.id_quartier "
            "JOIN Arret AS A ON AQ.id_arret = A.id_arret "
            "JOIN Capteur AS C ON A.id_arret = C.id_arret "
            "JOIN Mesure AS M ON C.id_capteur = M.id_capteur "
            "WHERE C.type_capteur = 'Bruit' "
            "GROUP BY Q.nom "
            "ORDER BY moyenne_bruit_db DESC "
            "LIMIT 5;"
        ),
        "F": (
            "SELECT DISTINCT L.nom_ligne "
            "FROM Ligne AS L "
            "JOIN Trafic AS T ON L.id_ligne = T.id_ligne "
            "WHERE T.retard_minutes > 10 "
            "EXCEPT "
            "SELECT DISTINCT L.nom_ligne "
            "FROM Ligne AS L "
            "JOIN Trafic AS T ON L.id_ligne = T.id_ligne "
            "JOIN Incident AS I ON T.id_trafic = I.id_trafic;"
        ),
        "G": (
            "SELECT "
            "COUNT(*) AS total_trajets, "
            "SUM(CASE WHEN retard_minutes = 0 THEN 1 ELSE 0 END) "
            "AS trajets_sans_retard, "
            "(CAST(SUM(CASE WHEN retard_minutes = 0 THEN 1 ELSE 0 END) "
            "AS REAL) / COUNT(*)) * 100 "
            "AS taux_ponctualite_global_pourcent "
            "FROM Trafic;"
        ),
        "H": (
            "SELECT Q.nom, "
            "COUNT(AQ.id_arret) AS nombre_arrets "
            "FROM Quartier AS Q "
            "LEFT JOIN ArretQuartier AS AQ "
            "ON Q.id_quartier = AQ.id_quartier "
            "GROUP BY Q.nom "
            "ORDER BY nombre_arrets DESC;"
        ),
        "I": (
            "WITH AvgRetard AS ( "
            "  SELECT id_ligne, "
            "         AVG(retard_minutes) AS moyenne_retard "
            "  FROM Trafic "
            "  GROUP BY id_ligne "
            "), "
            "AvgCO2 AS ( "
            "  SELECT A.id_ligne, "
            "         AVG(M.valeur) AS moyenne_co2 "
            "  FROM Mesure AS M "
            "  JOIN Capteur AS C ON M.id_capteur = C.id_capteur "
            "  JOIN Arret AS A ON C.id_arret = A.id_arret "
            "  WHERE C.type_capteur = 'CO2' "
            "  GROUP BY A.id_ligne "
            ") "
            "SELECT L.nom_ligne, "
            "COALESCE(R.moyenne_retard, 0) AS moyenne_retard, "
            "COALESCE(C.moyenne_co2, 0) AS moyenne_co2 "
            "FROM Ligne AS L "
            "LEFT JOIN AvgRetard AS R ON L.id_ligne = R.id_ligne "
            "LEFT JOIN AvgCO2 AS C ON L.id_ligne = C.id_ligne "
            "ORDER BY L.nom_ligne;"
        ),
        "J": (
            "SELECT L.nom_ligne, "
            "AVG(M.valeur) AS moyenne_temperature "
            "FROM Ligne AS L "
            "JOIN Arret AS A ON L.id_ligne = A.id_ligne "
            "JOIN Capteur AS C ON A.id_arret = C.id_arret "
            "JOIN Mesure AS M ON C.id_capteur = M.id_capteur "
            "WHERE C.type_capteur = 'Temperature' "
            "GROUP BY L.nom_ligne "
            "ORDER BY moyenne_temperature DESC;"
        ),
        "K": (
            "SELECT C.nom, "
            "AVG(T.retard_minutes) AS moyenne_retard_minutes "
            "FROM Chauffeur AS C "
            "JOIN Vehicule AS V ON C.id_chauffeur = V.id_chauffeur "
            "JOIN Trafic AS T ON V.id_ligne = T.id_ligne "
            "GROUP BY C.nom "
            "ORDER BY moyenne_retard_minutes DESC;"
        ),
        "L": (
            "SELECT L.nom_ligne, "
            "COUNT(V.id_vehicule) AS total_vehicules, "
            "SUM(CASE WHEN V.type_vehicule = 'Electrique' THEN 1 ELSE 0 END) "
            "AS nb_electriques, "
            "(CAST(SUM(CASE WHEN V.type_vehicule = 'Electrique' THEN 1 "
            "ELSE 0 END) AS REAL) / COUNT(V.id_vehicule)) * 100 "
            "AS pourcentage_electrique "
            "FROM Ligne AS L "
            "JOIN Vehicule AS V ON L.id_ligne = V.id_ligne "
            "WHERE L.type = 'Bus' "
            "GROUP BY L.nom_ligne "
            "ORDER BY pourcentage_electrique DESC;"
        ),
        "M": (
            "SELECT C.id_capteur, "
            "C.latitude, "
            "C.longitude, "
            "AVG(M.valeur) AS moyenne_co2, "
            "CASE "
            "  WHEN AVG(M.valeur) > 800 THEN 'Élevé' "
            "  WHEN AVG(M.valeur) > 450 THEN 'Moyen' "
            "  ELSE 'Faible' "
            "END AS niveau_pollution "
            "FROM Capteur AS C "
            "JOIN Mesure AS M ON C.id_capteur = M.id_capteur "
            "WHERE C.type_capteur = 'CO2' "
            "GROUP BY C.id_capteur, C.latitude, C.longitude "
            "ORDER BY moyenne_co2 DESC;"
        ),
        "N": (
            "SELECT nom_ligne, "
            "type, "
            "frequentation_moyenne, "
            "CASE "
            "  WHEN frequentation_moyenne > 2000 THEN 'Haute Fréquentation' "
            "  WHEN frequentation_moyenne > 1000 THEN 'Moyenne Fréquentation' "
            "  ELSE 'Basse Fréquentation' "
            "END AS categorie_frequentation "
            "FROM Ligne "
            "ORDER BY frequentation_moyenne DESC;"
        ),
    }

    with sqlite3.connect(DB_FILE) as conn:
        for code, query in sql_queries.items():
            try:
                df_result = pd.read_sql_query(query, conn)
                enregistrer_resultats_csv(
                    DOSSIER_CSV,
                    f"resultat_req_{code.lower()}.csv",
                    df_result,
                )
                resultats[code] = df_result
            except Exception as exc:
                resultats[code] = pd.DataFrame([{"erreur": str(exc)}])

    return resultats


def charger_cache_csv() -> tuple[Dict[str, pd.DataFrame], bool]:
    """
    Recharge, si disponibles, les résultats SQL précédemment exportés en CSV.

    Retour
    ------
    (dict[str, pandas.DataFrame], bool)
        - Dictionnaire des résultats trouvés (par code de requête).
        - Booléen indiquant si au moins un CSV a été retrouvé.
    """
    print("Chargement cache CSV...", end="\n")
    resultats: Dict[str, pd.DataFrame] = {}
    found_any = False

    for code in REQUETES_OBJECTIFS.keys():
        path = os.path.join(DOSSIER_CSV, f"resultat_req_{code.lower()}.csv")
        if os.path.exists(path):
            try:
                resultats[code] = pd.read_csv(path)
                found_any = True
            except Exception:
                resultats[code] = pd.DataFrame()

    if found_any:
        print("CSV chargés ✅")
    else:
        print(" Non trouve")
    print("\n")
    return resultats, found_any


def charger_cache_csv_mongo() -> tuple[Dict[str, pd.DataFrame], bool]:
    """
    Recharge, si disponibles, les résultats MongoDB précédemment exportés en CSV.

    Retour
    ------
    (dict[str, pandas.DataFrame], bool)
        - Dictionnaire des résultats trouvés (par code de requête).
        - Booléen indiquant si au moins un CSV a été retrouvé.
    """
    print("Chargement cache CSV MongoDB...", end="\n")
    resultats: Dict[str, pd.DataFrame] = {}
    found_any = False

    for code in REQUETES_OBJECTIFS.keys():
        path = os.path.join(
            DOSSIER_MONGO_CSV,
            f"resultat_req_{code.lower()}.csv",
        )
        if os.path.exists(path):
            try:
                resultats[code] = pd.read_csv(path)
                found_any = True
            except Exception:
                resultats[code] = pd.DataFrame()

    if found_any:
        print("CSV MongoDB chargés ✅")
    else:
        print(" Non trouve")
    print("\n")
    return resultats, found_any

# =====================================================================
# ETAT ET COMPOSANTS STREAMLIT
# =====================================================================

#            entre logique métier (ETL / requêtes) et présentation.


# Création des dossiers nécessaires au démarrage de l'application.
os.makedirs(DOSSIER_DATA, exist_ok=True)
os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
os.makedirs(DOSSIER_CSV, exist_ok=True)
os.makedirs(DOSSIER_JSON, exist_ok=True)
os.makedirs(DOSSIER_MONGO_CSV, exist_ok=True)

def init_session_state() -> None:
    """
    Initialise les variables de session Streamlit.
    """
    if st.session_state.get("initialized", False):
        return

    # --- AJOUT GESTION API KEY ---
    # On charge la clé du .env par défaut dans la session
    if "groq_api_key" not in st.session_state:
        st.session_state["groq_api_key"] = os.getenv("GROQ_API_KEY", "")
    # -----------------------------

    st.session_state["requetes_objectifs"] = REQUETES_OBJECTIFS

    resultats_sql, sql_cache_found = charger_cache_csv()
    st.session_state["resultats_sql"] = resultats_sql
    st.session_state["queries_sql_executed"] = sql_cache_found

    resultats_mongo, mongo_cache_found = charger_cache_csv_mongo()
    st.session_state["resultats_mongo"] = resultats_mongo
    st.session_state["queries_mongo_executed"] = mongo_cache_found

    st.session_state["migration_logs"] = []
    st.session_state["migration_done_msg"] = ""
    st.session_state["migration_running"] = False
    
    st.session_state["ai_json_response"] = None 
    st.session_state["ai_question_text_value"] = ""

    st.session_state["initialized"] = True

# =====================================================================
# Partie 1 : REQUÊTES SQLITE
# =====================================================================

def render_partie_1_sqlite(tab) -> None:
    """
    Affiche la Partie 1 : exécution et visualisation des requêtes SQLite.

    Paramètres
    ----------
    tab :
        Conteneur Streamlit (onglet) dans lequel les éléments sont rendus.
    """
    with tab:
        st.subheader("Partie 1 : Requêtes SQLite")

        status_text = (
            "Données chargées."
            if st.session_state["queries_sql_executed"]
            else "Données non chargées."
        )
        st.write(status_text)

        if st.button("Executer Requetes", key="btn_sql_run"):
            with st.spinner("Exécution des requêtes SQLite..."):
                res = executer_toutes_les_requetes()
            st.session_state["resultats_sql"] = res
            st.session_state["queries_sql_executed"] = True
            st.success("✅ Requêtes terminées.")

        st.markdown("---")

        if not st.session_state["queries_sql_executed"]:
            st.info(
                "Les résultats ne sont pas encore disponibles. "
                "Cliquez sur « Executer Requetes » pour lancer les requêtes.",
            )
            return

        st.markdown("### Résultats détaillés des requêtes SQLite")

        for code, objectif in st.session_state["requetes_objectifs"].items():
            df = st.session_state["resultats_sql"].get(code)
            with st.expander(
                f"Requête {code} – {objectif}",
                expanded=False,
            ):
                st.markdown(f"**Objectif :** {objectif}")
                if df is None:
                    st.warning("Aucun résultat pour cette requête.")
                elif df.empty:
                    st.info(
                        "La requête n'a retourné aucun enregistrement.",
                    )
                else:
                    st.dataframe(
                        df.style.set_properties(
                            **{"text-align": "left"},
                        ),
                        width="content",
                    )

# =====================================================================
# Partie 2 : MIGRATION SQLITE -> MONGODB
# =====================================================================

def streamlit_migration_log(
    message: str,
    replace_last: bool = False,
) -> None:
    """
    Fonction de log spécifique à la migration, synchronisant le journal
    en temps réel avec l'interface Streamlit.

    Paramètres
    ----------
    message : str
        Message à afficher dans le journal de migration.
    replace_last : bool
        Si True, remplace la dernière entrée (progression), sinon ajoute
        une nouvelle ligne.
    """
    global MIGRATION_LOG_PLACEHOLDER

    logs: List[str] = st.session_state.get("migration_logs", [])
    if replace_last and logs:
        logs[-1] = message
    else:
        logs.append(message)
    st.session_state["migration_logs"] = logs

    if MIGRATION_LOG_PLACEHOLDER is not None:
        display_lines = logs[-MAX_LOG_LINES:]
        text_content = "\n".join(display_lines)
        MIGRATION_LOG_PLACEHOLDER.code(text_content, language="text")
        

def render_partie_2_migration(tab) -> None:
    """
    Affiche la Partie 2 : pilotage de la migration SQLite -> MongoDB.

    Paramètres
    ----------
    tab :
        Conteneur Streamlit (onglet) dans lequel les éléments sont rendus.
    """
    global MIGRATION_LOG_PLACEHOLDER

    with tab:
        st.subheader("Partie 2 : Migration vers MongoDB")
        st.caption(
            "Cliquez pour migrer les données de SQLite vers MongoDB.",
        )

        def start_migration_callback() -> None:
            """
            Callback appelé au clic sur le bouton de migration pour
            marquer le démarrage du traitement.
            """
            st.session_state["migration_running"] = True
            st.session_state["migration_logs"] = []

        col_btn, col_status = st.columns([1, 3], gap="small")

        with col_btn:
            st.button(
                "Lancer Migration",
                key="btn_migration",
                width="content",
                on_click=start_migration_callback,
                disabled=st.session_state["migration_running"],
            )

        MIGRATION_LOG_PLACEHOLDER = st.empty()

        if st.session_state["migration_running"]:
            MIGRATION_LOG_PLACEHOLDER.code(
                "Initialisation du processus...",
                language="text",
            )

            with col_status:
                with st.spinner(
                    "Migration en cours... Le bouton est désactivé.",
                ):
                    migrer_sqlite_vers_mongo(log_fn_raw=streamlit_migration_log)

            st.session_state["migration_done_msg"] = (
                "Migration terminée avec succès !"
            )
            st.session_state["migration_running"] = False

            st.toast("Migration terminée !", icon="🎉")
            time.sleep(1)
            st.rerun()

        if st.session_state.get("migration_done_msg"):
            with col_status:
                st.success(st.session_state["migration_done_msg"])

        logs: List[str] = st.session_state.get("migration_logs", [])
        if logs:
            log_text = "\n".join(logs[-MAX_LOG_LINES:])
        else:
            log_text = "Prêt à lancer la migration."

        MIGRATION_LOG_PLACEHOLDER.text_area(
            label="Journal de migration",
            value=log_text,
            height=400,
        )

# =====================================================================
# REQUETES MONGODB (PARTIE 3)
# =====================================================================
# =====================================================================
# REQUETES MONGODB (PARTIE 3)
# =====================================================================

def query_A_mongo(db) -> pd.DataFrame:
    """
    Requête A (MongoDB).
    SQL: nom_ligne, moyenne_retard_minutes
    """
    pipeline = [
        {"$unwind": "$trafic"},
        {"$match": {"trafic.retard_minutes": {"$ne": None}}},
        {
            "$group": {
                "_id": "$nom_ligne",
                "moyenne_retard_minutes": {"$avg": "$trafic.retard_minutes"},
            },
        },
        {"$sort": {"moyenne_retard_minutes": -1}},
        {
            "$project": {
                "_id": 0,
                "nom_ligne": "$_id",
                "moyenne_retard_minutes": 1,
            },
        },
    ]
    df = aggregate_to_df(db.lignes, pipeline)
    # Force l'ordre des colonnes
    return df[["nom_ligne", "moyenne_retard_minutes"]] if not df.empty else df

def query_B_mongo(db):
    """
    Requête B en MongoDB :
    Estimer le nombre moyen de passagers transportés par jour pour chaque ligne.
    Retourne un DataFrame avec :
        - id_ligne
        - moyenne_passagers_jour
    """
    pipeline = [
        {"$unwind": "$arrets"},
        {"$unwind": "$arrets.horaires"},
        {
            "$project": {
                "_id": 0,
                "id_ligne": "$id_ligne",
                "jour": {
                    "$substrBytes": ["$arrets.horaires.heure_prevue", 0, 10]
                },
                "passagers_estimes": "$arrets.horaires.passagers_estimes",
            }
        },
        {
            "$group": {
                "_id": {"id_ligne": "$id_ligne", "jour": "$jour"},
                "total_passagers_jour": {"$sum": "$passagers_estimes"},
            }
        },
        {
            "$group": {
                "_id": "$_id.id_ligne",
                "moyenne_passagers_jour": {"$avg": "$total_passagers_jour"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "id_ligne": "$_id",
                "moyenne_passagers_jour": 1,
            }
        },
        {"$sort": {"moyenne_passagers_jour": -1}},
    ]

    df = aggregate_to_df(db.lignes, pipeline)
    if df.empty:
        return df
    return df[["id_ligne", "moyenne_passagers_jour"]]

def query_C_mongo(db) -> pd.DataFrame:
    """
    Requête C (MongoDB).
    SQL: nom_ligne, nb_trafic_avec_incident, nb_total_trafic, taux_incident_pourcent
    """
    pipeline = [
        {
            "$unwind": {
                "path": "$trafic",
                "preserveNullAndEmptyArrays": True,
            },
        },
        {
            "$group": {
                "_id": {
                    "id_ligne": "$id_ligne",
                    "nom_ligne": "$nom_ligne",
                    "id_trafic": "$trafic.id_trafic",
                },
                "has_incident": {
                    "$max": {
                        "$cond": [
                            {
                                "$gt": [
                                    {
                                        "$size": {
                                            "$ifNull": [
                                                "$trafic.incidents",
                                                [],
                                            ],
                                        }
                                    },
                                    0,
                                ],
                            },
                            1,
                            0,
                        ],
                    },
                },
            },
        },
        {
            "$group": {
                "_id": "$_id.nom_ligne",
                "nb_trafic_avec_incident": {"$sum": "$has_incident"},
                "nb_total_trafic": {
                    "$sum": {
                        "$cond": [{"$ifNull": ["$_id.id_trafic", False]}, 1, 0],
                    },
                },
            },
        },
        {
            "$addFields": {
                "taux_incident_pourcent": {
                    "$cond": [
                        {"$eq": ["$nb_total_trafic", 0]},
                        0,
                        {
                            "$multiply": [
                                {
                                    "$divide": [
                                        "$nb_trafic_avec_incident",
                                        "$nb_total_trafic",
                                    ],
                                },
                                100,
                            ],
                        },
                    ],
                },
            },
        },
        {"$sort": {"taux_incident_pourcent": -1}},
        {
            "$project": {
                "_id": 0,
                "nom_ligne": "$_id",
                "nb_trafic_avec_incident": 1,
                "nb_total_trafic": 1,
                "taux_incident_pourcent": 1,
            },
        },
    ]
    df = aggregate_to_df(db.lignes, pipeline)
    cols = ["nom_ligne", "nb_trafic_avec_incident", "nb_total_trafic", "taux_incident_pourcent"]
    return df[cols] if not df.empty else df

def query_D_mongo(db) -> pd.DataFrame:
    """
    Requête D (MongoDB) - ULTRA OPTIMISÉE.
    Utilise 'vehicules_cache' et 'co2_moyen_ligne'.
    Complexité: O(L * V_per_line) au lieu de O(L * Arrets * Horaires).
    """
    pipeline = [
        # 1. On ne prend que les lignes qui ont une moyenne CO2 et des véhicules
        {
            "$match": {
                "co2_moyen_ligne": {"$exists": True, "$ne": None},
                "vehicules_cache": {"$exists": True, "$ne": []}
            }
        },
        # 2. On garde juste ce dont on a besoin
        {
            "$project": {
                "vehicules_cache": 1,
                "co2_moyen_ligne": 1
            }
        },
        # 3. On "sort" les véhicules de leur tableau
        {"$unwind": "$vehicules_cache"},
        
        # 4. Projection finale : le véhicule hérite du CO2 de sa ligne
        {
            "$project": {
                "_id": 0,
                "id_vehicule": "$vehicules_cache.id_vehicule",
                "immatriculation": "$vehicules_cache.immatriculation",
                "moyenne_co2": "$co2_moyen_ligne"
            }
        },
        # 5. Tri
        {"$sort": {"moyenne_co2": -1}}
    ]

    df = aggregate_to_df(db.lignes, pipeline)
    
    if df.empty:
        return pd.DataFrame(columns=["id_vehicule", "immatriculation", "moyenne_co2"])
        
    return df[["id_vehicule", "immatriculation", "moyenne_co2"]]

def query_E_mongo(db) -> pd.DataFrame:
    """
    Requête E (MongoDB).
    SQL: nom, moyenne_bruit_db
    """
    pipeline = [
        {
            "$lookup": {
                "from": "capteurs",
                "localField": "arrets.id_arret",
                "foreignField": "arret.id_arret",
                "as": "caps",
            },
        },
        {"$unwind": "$caps"},
        {"$match": {"caps.type_capteur": "Bruit"}},
        {"$unwind": "$caps.mesures"},
        {
            "$group": {
                "_id": "$nom",
                "moyenne_bruit_db": {"$avg": "$caps.mesures.valeur"},
            },
        },
        {"$sort": {"moyenne_bruit_db": -1}},
        {"$limit": 5},
        {
            "$project": {
                "_id": 0,
                "nom": "$_id",
                "moyenne_bruit_db": 1,
            },
        },
    ]
    df = aggregate_to_df(db.quartiers, pipeline)
    return df[["nom", "moyenne_bruit_db"]] if not df.empty else df


def query_F_mongo(db) -> pd.DataFrame:
    """
    Requête F (MongoDB).
    SQL: nom_ligne (DISTINCT)
    """
    pipeline = [
        {
            "$project": {
                "nom_ligne": 1,
                "has_big_delay": {
                    "$gt": [
                        {
                            "$size": {
                                "$filter": {
                                    "input": {"$ifNull": ["$trafic", []]},
                                    "as": "t",
                                    "cond": {
                                        "$gt": ["$$t.retard_minutes", 10],
                                    },
                                },
                            },
                        },
                        0,
                    ],
                },
                "has_incident": {
                    "$gt": [
                        {
                            "$size": {
                                "$filter": {
                                    "input": {"$ifNull": ["$trafic", []]},
                                    "as": "t",
                                    "cond": {
                                        "$gt": [
                                            {
                                                "$size": {
                                                    "$ifNull": [
                                                        "$$t.incidents",
                                                        [],
                                                    ]
                                                }
                                            },
                                            0,
                                        ],
                                    },
                                },
                            },
                        },
                        0,
                    ],
                },
            },
        },
        {
            "$match": {
                "has_big_delay": True,
                "has_incident": False,
            },
        },
        {
            "$project": {
                "_id": 0,
                "nom_ligne": 1,
            },
        },
    ]
    df = aggregate_to_df(db.lignes, pipeline)
    return df[["nom_ligne"]] if not df.empty else df


def query_G_mongo(db) -> pd.DataFrame:
    """
    Requête G (MongoDB).
    SQL: total_trajets, trajets_sans_retard, taux_ponctualite_global_pourcent
    """
    pipeline = [
        {"$unwind": "$trafic"},
        {
            "$group": {
                "_id": None,
                "total_trajets": {"$sum": 1},
                "trajets_sans_retard": {
                    "$sum": {
                        "$cond": [{"$eq": ["$trafic.retard_minutes", 0]}, 1, 0],
                    },
                },
            },
        },
        {
            "$addFields": {
                "taux_ponctualite_global_pourcent": {
                    "$cond": [
                        {"$eq": ["$total_trajets", 0]},
                        0,
                        {
                            "$multiply": [
                                {
                                    "$divide": [
                                        "$trajets_sans_retard",
                                        "$total_trajets",
                                    ],
                                },
                                100,
                            ],
                        },
                    ],
                },
            },
        },
        {
            "$project": {
                "_id": 0,
                "total_trajets": 1,
                "trajets_sans_retard": 1,
                "taux_ponctualite_global_pourcent": 1,
            },
        },
    ]
    df = aggregate_to_df(db.lignes, pipeline)
    cols = ["total_trajets", "trajets_sans_retard", "taux_ponctualite_global_pourcent"]
    return df[cols] if not df.empty else df


def query_H_mongo(db) -> pd.DataFrame:
    """
    Requête H (MongoDB).
    SQL: nom, nombre_arrets
    """
    pipeline = [
        {
            "$addFields": {
                "nombre_arrets": {"$size": {"$ifNull": ["$arrets", []]}},
            },
        },
        {"$sort": {"nombre_arrets": -1}},
        {
            "$project": {
                "_id": 0,
                "nom": 1,
                "nombre_arrets": 1,
            },
        },
    ]
    df = aggregate_to_df(db.quartiers, pipeline)
    return df[["nom", "nombre_arrets"]] if not df.empty else df


def query_I_mongo(db) -> pd.DataFrame:
    """
    Requête I (MongoDB).
    SQL: nom_ligne, moyenne_retard, moyenne_co2
    """
    pipeline = [
        {
            "$addFields": {
                "moyenne_retard": {
                    "$cond": [
                        {
                            "$gt": [
                                {"$size": {"$ifNull": ["$trafic", []]}},
                                0,
                            ],
                        },
                        {"$avg": "$trafic.retard_minutes"},
                        0,
                    ],
                },
            },
        },
        {
            "$lookup": {
                "from": "capteurs",
                "localField": "id_ligne",
                "foreignField": "arret.id_ligne",
                "as": "capteurs_ligne",
            },
        },
        {
            "$addFields": {
                "mesures_co2": {
                    "$reduce": {
                        "input": "$capteurs_ligne",
                        "initialValue": [],
                        "in": {
                            "$cond": [
                                {"$eq": ["$$this.type_capteur", "CO2"]},
                                {
                                    "$concatArrays": [
                                        "$$value",
                                        {
                                            "$map": {
                                                "input": {
                                                    "$ifNull": [
                                                        "$$this.mesures",
                                                        [],
                                                    ]
                                                },
                                                "as": "m",
                                                "in": "$$m.valeur",
                                            }
                                        },
                                    ]
                                },
                                "$$value",
                            ],
                        },
                    },
                },
            },
        },
        {
            "$addFields": {
                "moyenne_co2": {
                    "$cond": [
                        {"$gt": [{"$size": "$mesures_co2"}, 0]},
                        {"$avg": "$mesures_co2"},
                        0,
                    ],
                },
            },
        },
        {"$sort": {"nom_ligne": 1}},
        {
            "$project": {
                "_id": 0,
                "nom_ligne": 1,
                "moyenne_retard": 1,
                "moyenne_co2": 1,
            },
        },
    ]
    df = aggregate_to_df(db.lignes, pipeline)
    return df[["nom_ligne", "moyenne_retard", "moyenne_co2"]] if not df.empty else df


def query_J_mongo(db) -> pd.DataFrame:
    """
    Requête J (MongoDB).
    SQL: nom_ligne, moyenne_temperature
    """
    pipeline = [
        {"$match": {"type_capteur": "Temperature"}},
        {"$unwind": "$mesures"},
        {
            "$group": {
                "_id": "$arret.id_ligne",
                "moyenne_temperature": {"$avg": "$mesures.valeur"},
            },
        },
        {
            "$lookup": {
                "from": "lignes",
                "localField": "_id",
                "foreignField": "id_ligne",
                "as": "ligne",
            },
        },
        {"$unwind": "$ligne"},
        {"$sort": {"moyenne_temperature": -1}},
        {
            "$project": {
                "_id": 0,
                "nom_ligne": "$ligne.nom_ligne",
                "moyenne_temperature": 1,
            },
        },
    ]
    df = aggregate_to_df(db.capteurs, pipeline)
    return df[["nom_ligne", "moyenne_temperature"]] if not df.empty else df

def query_K_mongo(db) -> pd.DataFrame:
    """
    Requête K (MongoDB) - ULTRA OPTIMISÉE.
    Utilise 'chauffeurs_cache' et 'stats_trafic' (pré-calculé).
    Évite totalement de lire le tableau 'trafic'.
    """
    pipeline = [
        # 1. Filtre : Lignes avec chauffeurs et stats trafic existantes
        { 
            "$match": { 
                "stats_trafic": { "$exists": True },
                "chauffeurs_cache": { "$exists": True, "$ne": [] }
            } 
        },

        # 2. Projection légère
        { 
            "$project": {
                "chauffeurs_cache": 1,
                "total_retard": "$stats_trafic.total_retard",
                "nb_trajets": "$stats_trafic.nb_trajets"
            }
        },

        # 3. On déroule la liste des chauffeurs (liste très courte)
        { "$unwind": "$chauffeurs_cache" },

        # 4. Groupement par chauffeur 
        # (Si un chauffeur est sur 2 lignes, on additionne les stats pré-calculées)
        { 
            "$group": {
                "_id": "$chauffeurs_cache.nom_chauffeur",
                "cumul_retard": { "$sum": "$total_retard" },
                "cumul_trajets": { "$sum": "$nb_trajets" }
            }
        },

        # 5. Calcul final de la moyenne
        { 
            "$project": {
                "_id": 0,
                "nom": "$_id",
                "moyenne_retard_minutes": { 
                    "$cond": [
                        { "$eq": ["$cumul_trajets", 0] },
                        0,
                        { "$divide": ["$cumul_retard", "$cumul_trajets"] }
                    ]
                }
            }
        },

        # 6. Tri
        { "$sort": { "moyenne_retard_minutes": -1 } }
    ]

    df = aggregate_to_df(db.lignes, pipeline)
    return df[["nom", "moyenne_retard_minutes"]] if not df.empty else df

def query_L_mongo(db) -> pd.DataFrame:
    """
    Requête L (MongoDB) - CORRIGÉE (DÉDUPLICATION VÉHICULES)
    Objectif : % Bus électriques.
    SQL : nom_ligne, total_vehicules, nb_electriques, pourcentage_electrique
    """
    pipeline = [
        {"$match": {"type": "Bus"}},
        {"$unwind": "$arrets"},
        {"$unwind": "$arrets.horaires"},
        
        {"$match": {"arrets.horaires.vehicule.id_vehicule": {"$ne": None}}},

        # --- DÉDUPLICATION ---
        # On veut compter les VÉHICULES uniques par ligne, pas les horaires.
        {
            "$group": {
                "_id": {
                    "nom_ligne": "$nom_ligne",
                    "id_vehicule": "$arrets.horaires.vehicule.id_vehicule"
                },
                "type_vehicule": {"$first": "$arrets.horaires.vehicule.type_vehicule"}
            }
        },
        # ---------------------

        # Maintenant on compte les véhicules uniques
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
                        ],
                    },
                },
            },
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
                                    ],
                                },
                                100,
                            ],
                        },
                    ],
                },
            },
        },
        {"$sort": {"pourcentage_electrique": -1}},
        {
            "$project": {
                "_id": 0,
                "nom_ligne": "$_id",
                "total_vehicules": 1,
                "nb_electriques": 1,
                "pourcentage_electrique": 1,
            },
        },
    ]
    df = aggregate_to_df(db.lignes, pipeline)
    cols = ["nom_ligne", "total_vehicules", "nb_electriques", "pourcentage_electrique"]
    return df[cols] if not df.empty else df
def query_M_mongo(db) -> pd.DataFrame:
    """
    Requête M (MongoDB).
    SQL: id_capteur, latitude, longitude, moyenne_co2, niveau_pollution
    """
    pipeline = [
        {"$match": {"type_capteur": "CO2"}},
        {"$unwind": "$mesures"},
        {
            "$group": {
                "_id": {
                    "id_capteur": "$id_capteur",
                    "position": "$position",
                },
                "moyenne_co2": {"$avg": "$mesures.valeur"},
            },
        },
        {
            "$addFields": {
                "niveau_pollution": {
                    "$switch": {
                        "branches": [
                            {
                                "case": {"$gt": ["$moyenne_co2", 800]},
                                "then": "Élevé",
                            },
                            {
                                "case": {"$gt": ["$moyenne_co2", 450]},
                                "then": "Moyen",
                            },
                        ],
                        "default": "Faible",
                    },
                },
            },
        },
        {"$sort": {"moyenne_co2": -1}},
        {
            "$project": {
                "_id": 0,
                "id_capteur": "$_id.id_capteur",
                "latitude": {"$arrayElemAt": ["$_id.position.coordinates", 1]},
                "longitude": {"$arrayElemAt": ["$_id.position.coordinates", 0]},
                "moyenne_co2": 1,
                "niveau_pollution": 1,
            },
        },
    ]
    df = aggregate_to_df(db.capteurs, pipeline)
    cols = ["id_capteur", "latitude", "longitude", "moyenne_co2", "niveau_pollution"]
    return df[cols] if not df.empty else df


def query_N_mongo(db) -> pd.DataFrame:
    """
    Requête N (MongoDB).
    SQL: nom_ligne, type, frequentation_moyenne, categorie_frequentation
    """
    pipeline = [
        {
            "$addFields": {
                "categorie_frequentation": {
                    "$switch": {
                        "branches": [
                            {
                                "case": {
                                    "$gt": [
                                        "$frequentation_moyenne",
                                        2000,
                                    ],
                                },
                                "then": "Haute Fréquentation",
                            },
                            {
                                "case": {
                                    "$gt": [
                                        "$frequentation_moyenne",
                                        1000,
                                    ],
                                },
                                "then": "Moyenne Fréquentation",
                            },
                        ],
                        "default": "Basse Fréquentation",
                    },
                },
            },
        },
        {"$sort": {"frequentation_moyenne": -1}},
        {
            "$project": {
                "_id": 0,
                "nom_ligne": 1,
                "type": 1,
                "frequentation_moyenne": 1,
                "categorie_frequentation": 1,
            },
        },
    ]
    df = aggregate_to_df(db.lignes, pipeline)
    cols = ["nom_ligne", "type", "frequentation_moyenne", "categorie_frequentation"]
    return df[cols] if not df.empty else df

QUERY_MONGO_FUNCS: Dict[str, Callable] = {
    "A": query_A_mongo,
    "B": query_B_mongo,
    "C": query_C_mongo,
    "D": query_D_mongo,
    "E": query_E_mongo,
    "F": query_F_mongo,
    "G": query_G_mongo,
    "H": query_H_mongo,
    "I": query_I_mongo,
    "J": query_J_mongo,
    "K": query_K_mongo,
    "L": query_L_mongo,
    "M": query_M_mongo,
    "N": query_N_mongo,
}


def executer_toutes_les_requetes_mongo() -> Dict[str, pd.DataFrame]:
    """
    Exécute les requêtes A -> N sur la base MongoDB Paris2055 et
    sauvegarde les résultats au format CSV.

    Retour
    ------
    dict[str, pandas.DataFrame]
        Dictionnaire associant le code de requête à son DataFrame
        résultat. En cas d'erreur globale de connexion, toutes les
        entrées contiendront un DataFrame avec une colonne 'erreur'.
    """
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
    resultats: Dict[str, pd.DataFrame] = {}

    try:
        client.admin.command("ping")

        if MONGO_DB_NAME not in client.list_database_names():
            error_msg = (
                f"La base '{MONGO_DB_NAME}' n'existe pas. "
                "Lancez la migration (Partie 2)."
            )
            error_df = pd.DataFrame([{"erreur": error_msg}])
            return {code: error_df for code in QUERY_MONGO_FUNCS.keys()}

        db = client[MONGO_DB_NAME]
        for code, func in QUERY_MONGO_FUNCS.items():
            try:
                df = func(db)
            except Exception as exc:
                df = pd.DataFrame(
                    [{"erreur": f"Erreur requête: {str(exc)}"}],
                )

            enregistrer_resultats_csv(
                DOSSIER_MONGO_CSV,
                f"resultat_req_{code.lower()}.csv",
                df,
            )
            resultats[code] = df

    except Exception as exc:
        err_msg = f"Impossible de se connecter à MongoDB : {str(exc)}"
        print(f"[ERREUR] {err_msg}")
        error_df = pd.DataFrame([{"erreur": err_msg}])
        return {code: error_df for code in QUERY_MONGO_FUNCS.keys()}
    finally:
        client.close()

    return resultats

def render_partie_3_mongo(tab) -> None:
    """
    Affiche la Partie 3 : exécution et visualisation des requêtes MongoDB.

    Paramètres
    ----------
    tab :
        Conteneur Streamlit (onglet) dans lequel les éléments sont rendus.
    """
    with tab:
        st.subheader("Partie 3 : Requêtes MongoDB")

        server_ok, db_ok = check_connexion_details()

        if not server_ok:
            st.error(
                f"❌ Impossible de se connecter au serveur MongoDB sur {MONGO_URI}",
            )
        elif not db_ok:
            st.warning(
                f"⚠️ La base '{MONGO_DB_NAME}' n'existe pas encore. "
                "Lancez la migration en Partie 2.",
            )
        else:
            st.success(
                f"✅ Serveur connecté et base '{MONGO_DB_NAME}' détectée.",
            )

        st.markdown("---")

        mongo_queries_executed = st.session_state.get(
            "queries_mongo_executed",
            False,
        )
        resultats_mongo: Dict[str, pd.DataFrame] = st.session_state.get(
            "resultats_mongo",
            {},
        )

        btn_disabled = not (server_ok and db_ok)

        if st.button(
            "Executer Requetes MongoDB",
            key="btn_mongo_run",
            disabled=btn_disabled,
        ):
            with st.spinner("Exécution des requêtes MongoDB..."):
                resultats_mongo = executer_toutes_les_requetes_mongo()
            st.session_state["resultats_mongo"] = resultats_mongo
            st.session_state["queries_mongo_executed"] = True
            mongo_queries_executed = True
            st.success("✅ Requêtes MongoDB terminées.")

        if not mongo_queries_executed:
            st.info(
                "Clique sur « Executer Requetes MongoDB » "
                "pour lancer les requêtes.",
            )
            return

        st.markdown("### Résultats détaillés des requêtes MongoDB")

        for code, objectif in st.session_state["requetes_objectifs"].items():
            df = resultats_mongo.get(code)
            with st.expander(
                f"Requête {code} – {objectif}",
                expanded=False,
            ):
                st.markdown(f"**Objectif :** {objectif}")
                if df is None:
                    st.warning("Aucun résultat pour cette requête.")
                elif df.empty:
                    st.info(
                        "La requête n'a retourné aucun enregistrement.",
                    )
                else:
                    st.dataframe(
                        df.style.set_properties(
                            **{"text-align": "left"},
                        ),
                        width="content",
                    )
# =====================================================================
# Partie 4 : DASHBOARDS ET CARTOGRAPHIE
# =====================================================================

def render_partie_4_streamlit(tab) -> None:
    """
    Affiche la Partie 4 : espace réservé pour des dashboards et cartes.

    Paramètres
    ----------
    tab :
        Conteneur Streamlit (onglet) dans lequel les éléments sont rendus.
    """
    with tab:
        st.subheader("Partie 4 : Tableau de bord et cartographie")
        st.info(
            "Espace réservé pour des visualisations supplémentaires "
            "directement à partir des données de Paris 2055 "
            "(cartes, dashboards, etc.).",
        )

# =====================================================================
# Partie 5 : COMPARAISON SQL vs MONGODB
# =====================================================================

def comparer_dataframes_souple(df1: pd.DataFrame, df2: pd.DataFrame) -> tuple[str, str]:
    """
    Compare deux DataFrames de manière souple pour valider la migration.
    
    Retourne:
    - Un statut (icône).
    - Un message explicatif.
    """
    if df1 is None or df2 is None:
        return "❌", "Un des résultats est manquant."
    
    if df1.empty and df2.empty:
        return "✅", "Les deux résultats sont vides (cohérent)."
        
    if df1.empty or df2.empty:
        return "❌", f"Disparité : SQL a {len(df1)} lignes, Mongo a {len(df2)} lignes."

    # 1. Comparaison du nombre de lignes
    if len(df1) != len(df2):
        diff = abs(len(df1) - len(df2))
        return "⚠️", f"Différence de taille : {len(df1)} (SQL) vs {len(df2)} (Mongo). Écart : {diff}."

    # 2. Comparaison du nombre de colonnes
    if len(df1.columns) != len(df2.columns):
        return "⚠️", f"Colonnes différentes : {list(df1.columns)} vs {list(df2.columns)}."

    # 3. Tentative de comparaison stricte des valeurs (avec tolérance pour les arrondis)
    try:
        # On trie les données pour s'assurer qu'elles sont dans le même ordre
        # On suppose que la première colonne est la clé de tri (ex: nom_ligne)
        col_sort_1 = df1.columns[0]
        col_sort_2 = df2.columns[0]
        
        df1_sorted = df1.sort_values(by=col_sort_1).reset_index(drop=True)
        df2_sorted = df2.sort_values(by=col_sort_2).reset_index(drop=True)

        # On normalise les noms de colonnes pour la comparaison (ignorer casse)
        df1_sorted.columns = [c.lower() for c in df1_sorted.columns]
        df2_sorted.columns = [c.lower() for c in df2_sorted.columns]

        pd.testing.assert_frame_equal(
            df1_sorted, 
            df2_sorted, 
            check_dtype=False, # Ignore int vs float
            check_exact=False, # Tolère les erreurs d'arrondi minimes
            rtol=1e-3 # Tolérance relative de 0.1%
        )
        return "✅", "Contenu identique (valeurs et dimensions)."
    except AssertionError as e:
        # Si c'est juste une histoire de noms de colonnes ou de types, on considère que c'est acceptable
        return "⚠️", "Dimensions OK, mais valeurs légèrement différentes (arrondis ou types)."
    except Exception as e:
        return "❌", f"Erreur lors de la comparaison : {str(e)}"


def render_partie_5_comparaison(tab) -> None:
    """
    Affiche la Partie 5 : Comparaison côte à côte des résultats SQL et MongoDB.
    """
    with tab:
        st.subheader("Partie 5 : Validation de la Migration (SQL vs NoSQL)")
        st.markdown(
            "Cet onglet permet de vérifier si les requêtes MongoDB renvoient "
            "bien les mêmes données métier que les requêtes SQL d'origine."
        )

        # Vérification que les caches sont chargés
        sql_ready = st.session_state.get("queries_sql_executed", False)
        mongo_ready = st.session_state.get("queries_mongo_executed", False)

        if not sql_ready or not mongo_ready:
            st.warning("⚠️ Veuillez exécuter les requêtes de la **Partie 1** (SQL) et de la **Partie 3** (MongoDB) pour voir la comparaison.")
            return

        st.markdown("---")

        res_sql = st.session_state["resultats_sql"]
        res_mongo = st.session_state["resultats_mongo"]
        objectifs = st.session_state["requetes_objectifs"]

        # Compteurs pour le résumé
        total_ok = 0
        total_queries = len(objectifs)

        for code, objectif in objectifs.items():
            df_sql = res_sql.get(code)
            df_mongo = res_mongo.get(code)

            # Calcul du statut
            icon, message = comparer_dataframes_souple(df_sql, df_mongo)
            if icon == "✅":
                total_ok += 1

            # Affichage dans un expander
            with st.expander(f"{icon} Requête {code} : {objectif[:60]}..."):
                st.caption(f"**Objectif :** {objectif}")
                
                # Message de statut
                if icon == "✅":
                    st.success(f"Résultat : {message}")
                elif icon == "⚠️":
                    st.warning(f"Résultat : {message}")
                else:
                    st.error(f"Résultat : {message}")

                col_a, col_b = st.columns(2)
                
                with col_a:
                    st.markdown("**1. Résultat SQL (Source)**")
                    if df_sql is not None and not df_sql.empty:
                        st.dataframe(df_sql, width='stretch', height=200)
                        st.caption(f"Lignes : {len(df_sql)} | Colonnes : {len(df_sql.columns)}")
                    else:
                        st.info("Vide ou erreur.")

                with col_b:
                    st.markdown("**2. Résultat MongoDB (Cible)**")
                    if df_mongo is not None and not df_mongo.empty:
                        st.dataframe(df_mongo, width='stretch', height=200)
                        st.caption(f"Lignes : {len(df_mongo)} | Colonnes : {len(df_mongo.columns)}")
                    else:
                        st.info("Vide ou erreur.")

        st.markdown("---")
        
        # Score final de validation
        score = int((total_ok / total_queries) * 100)
        if score == 100:
            st.balloons()
            st.success(f"🏆 Migration validée à 100% ! ({total_ok}/{total_queries} requêtes identiques)")
        elif score > 80:
            st.success(f"✅ Migration validée à {score}% ({total_ok}/{total_queries} requêtes identiques)")
        else:
            st.error(f"❌ Attention : Seulement {score}% de correspondance ({total_ok}/{total_queries}). Vérifiez vos pipelines.")

# =====================================================================
# Partie 6 : ASSISTANT IA GROQ / LLAMA3
# =====================================================================
def interroger_groq(question: str) -> tuple[Optional[Dict], Optional[str]]:
    # --- MODIFICATION ICI : On récupère la clé depuis la session ou l'input ---
    api_key = st.session_state.get("groq_api_key", "")
    
    if not api_key or "gsk_" not in api_key:
        return None, "Clé API Groq manquante ou invalide. Vérifiez la sidebar."

    # On passe la clé dynamique au client
    client = Groq(api_key=api_key)

    try:
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": SCHEMA_CONTEXT},
                {"role": "user", "content": f"La question est : {question}"},
            ],
            temperature=0,
            stream=False,
            response_format={"type": "json_object"},
        )

        response_content = completion.choices[0].message.content
        data = json.loads(response_content)
        return data, None

    except Exception as exc:
        return None, str(exc)
    
def render_partie_6_ia(tab) -> None:
    """
    Affiche la Partie 6 : assistant IA pilotant la génération de requêtes
    MongoDB via Groq / Llama 3.
    """
    QUESTION_BUTTONS = [
        "la moyenne des retards (en minutes) pour chaque ligne de transport.",

        "le nombre moyen de passagers transportés par jour pour chaque ligne.",

        "le taux d'incidents (en pourcentage) pour chaque ligne, basé sur le nombre de trajets ayant signalé un incident.",

        "les 5 quartiers ayant la moyenne de niveau de bruit (en dB) la plus élevée, basée sur les capteurs de bruit aux arrêts."
    ]

    # Initialisation de l'état pour la réponse JSON de l'IA
    if "ai_json_response" not in st.session_state:
        st.session_state["ai_json_response"] = None

    with tab:
        st.subheader("Partie 6 : Assistant IA 🤖 (Powered by Groq/Llama3)")
        st.markdown(
            "Posez n'importe quelle question sur vos données. "
            "L'IA va générer la requête MongoDB complexe pour vous.",
        )

        # Zone de saisie manuelle : on NE modifie jamais la clé 'ai_question_input' dans le code
        question = st.text_area(
            "💬 Posez votre question :",
            key="ai_question_input",
            height=70,
        )

        # Affichage du dernier JSON généré par l'IA (si disponible)
        if st.session_state.get("ai_json_response"):
            with st.expander(
                f"Voir le dernier JSON généré par l'IA "
                f"(Cible : {st.session_state['ai_json_response'].get('collection', 'N/A')})",
                expanded=True,
            ):
                st.code(
                    json.dumps(
                        st.session_state["ai_json_response"],
                        indent=2,
                        ensure_ascii=False,
                    ),
                    language="json",
                )

        col_btn, _ = st.columns([1, 3])

        # Variable locale qui décidera si on lance l'IA dans ce run
        question_a_executer: Optional[str] = None

        # 1) Bouton principal : on utilise la question tapée dans la zone de texte
        if col_btn.button("✨ Générer & Exécuter", type="primary", key="btn_ia_run"):
            question_a_executer = question.strip()

        st.markdown("---")

        # 2) Boutons de questions rapides : on exécute directement le texte du bouton
        st.markdown("### Questions fréquentes :")
        cols = st.columns(len(QUESTION_BUTTONS))
        for i, question_text in enumerate(QUESTION_BUTTONS):
            if cols[i].button(question_text, key=f"quick_q_{i}"):
                question_a_executer = question_text

        # Si aucun bouton n'a été cliqué, on s'arrête là
        if question_a_executer is None:
            return

        question_a_executer = question_a_executer.strip()
        if not question_a_executer:
            st.warning("Veuillez écrire une question.")
            st.session_state["ai_json_response"] = None
            return

        st.markdown(f"**Question envoyée à l'IA :** {question_a_executer}")

        # Appel à Groq
        with st.spinner("L'IA analyse votre demande..."):
            result_ia, error = interroger_groq(question_a_executer)

        st.session_state["ai_json_response"] = result_ia

        if error:
            st.error(f"Erreur API/LLM : {error}")
            if "Clé API" in error:
                st.info(
                    "Allez sur https://console.groq.com pour avoir une clé gratuite !",
                )
            return

        collection_cible = result_ia.get("collection")
        pipeline: Optional[List] = result_ia.get("pipeline")

        if not pipeline:
            st.error(
                "❌ Requête non comprise ou non pertinente pour la base de données. "
                "Veuillez poser une question concernant les lignes, capteurs ou quartiers de Paris 2055.",
            )
            return

        st.success("Requête générée avec succès !")

        # Exécution MongoDB
        with st.spinner(
            f"Exécution sur la collection '{collection_cible}'...",
        ):
            try:
                client = pymongo.MongoClient(MONGO_URI)
                db = client[MONGO_DB_NAME]

                if collection_cible not in db.list_collection_names():
                    st.error(
                        "Erreur : L'IA veut chercher dans "
                        f"'{collection_cible}' mais cette collection "
                        "n'existe pas.",
                    )
                    client.close()
                    return

                collection = db[collection_cible]
                results = list(collection.aggregate(pipeline))
                client.close()

                if results:
                    st.markdown(f"### 📊 Résultats ({len(results)})")
                    df_res = pd.DataFrame(results)
                    if "_id" in df_res.columns:
                        df_res["_id"] = df_res["_id"].astype(str)
                    st.dataframe(df_res, width='stretch')
                else:
                    st.warning(
                        "La requête est valide syntaxiquement, "
                        "mais aucun résultat n'a été trouvé.",
                    )
            except Exception as exc:
                st.error(f"Erreur lors de l'exécution MongoDB : {exc}")

# =====================================================================
# MAIN STREAMLIT
# =====================================================================
def main() -> None:
    """
    Point d'entrée de l'application Streamlit Paris 2055.
    """
    st.set_page_config(
        page_title="Paris 2055 - Requêtes et Migration vers MongoDB",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    init_session_state()

    st.title("Paris 2055 - Requêtes et Migration vers MongoDB")

    with st.sidebar:
        # =================================================
        # 1. CONFIGURATION API (En premier)
        # =================================================
        st.header("🔑 Configuration API")
        
        # Champ de texte connecté au session_state
        new_key = st.text_input(
            label="Groq API Key",
            value=st.session_state["groq_api_key"],
            type="password", 
            help="Collez votre clé gsk_... ici. Elle sera utilisée pour les requêtes IA."
        )

        # Si l'utilisateur change la clé
        if new_key != st.session_state["groq_api_key"]:
            st.session_state["groq_api_key"] = new_key
            
            # 1. Mise à jour en mémoire (pour l'utilisation immédiate)
            os.environ["GROQ_API_KEY"] = new_key
            
            # 2. Mise à jour du fichier .env physique (pour la persistance)
            dotenv_path = ".env"
            try:
                # set_key crée le fichier s'il n'existe pas, ou met à jour la ligne si elle existe
                set_key(dotenv_path, "GROQ_API_KEY", new_key)
                st.success("Clé sauvegardée dans .env ! ✅")
            except Exception as e:
                st.warning(f"Clé active mais non sauvegardée dans .env : {e}")
            
            time.sleep(1)
            st.rerun()

        st.markdown("---")

        # =================================================
        # 2. ÉTAT DES BASES DE DONNÉES
        # =================================================
        st.header("📡 État des Bases de Données")

        # --- A. SQLITE (SOURCE) ---
        if os.path.exists(DB_FILE):
            st.success("Source SQLite : **Trouvée**", icon="📄")
        else:
            st.error("Source SQLite : **Introuvable**", icon="❌")

        # --- B. MONGODB (CIBLE) ---
        server_ok, db_ok = check_connexion_details()

        if server_ok:
            st.success("Serveur MongoDB : **Connecté**", icon="✅")
            
            if db_ok:
                # --- LOGIQUE D'INSPECTION DU CONTENU ---
                try:
                    # On ouvre une connexion temporaire pour compter
                    temp_client = pymongo.MongoClient(MONGO_URI)
                    temp_db = temp_client[MONGO_DB_NAME]
                    
                    # Liste des collections attendues
                    cols_to_check = ["lignes", "quartiers", "capteurs"]
                    details = []
                    is_empty = True
                    
                    for col_name in cols_to_check:
                        count = temp_db[col_name].count_documents({})
                        if count > 0:
                            is_empty = False
                            details.append(f"▪️ **{col_name}** : {count} docs \n")
                        else:
                            details.append(f"▪️ **{col_name}** : ⚠️ 0 doc \n")
                    
                    temp_client.close()

                    # Affichage conditionnel selon le contenu
                    if is_empty:
                        st.warning(f"Base '{MONGO_DB_NAME}' : **Vide**", icon="📭")
                    else:
                        st.success(f"Base '{MONGO_DB_NAME}' : **Remplie**", icon="🍃")
                    
                    # Affichage des détails dans un petit menu déroulant
                    with st.expander("Voir le contenu"):
                        st.markdown("\n".join(details))

                except Exception:
                    st.warning("Base existante (Lecture impossible)", icon="⚠️")
            else:
                st.warning(f"Base '{MONGO_DB_NAME}' : **Manquante**", icon="❌")
        else:
            st.error("Serveur MongoDB : **Déconnecté**", icon="❌")

        st.markdown("---")

        # =================================================
        # 3. ÉTAT DES CACHES (CSV)
        # =================================================
        st.header("🗂️ État des Caches (CSV)")

        # --- CACHE SQL ---
        if st.session_state.get("queries_sql_executed", False):
            st.success("Résultats SQL : **Chargés**", icon="✅")
        else:
            st.info("Résultats SQL : **Vides**", icon="⚪")

        # --- CACHE MONGODB ---
        if st.session_state.get("queries_mongo_executed", False):
            st.success("Résultats Mongo : **Chargés**", icon="✅")
        else:
            st.info("Résultats Mongo : **Vides**", icon="⚪")    
        
    # =================================================
    # CORPS PRINCIPAL (ONGLETS)
    # =================================================
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(
        [
            "Partie 1 : SQL",
            "Partie 2 : Migration",
            "Partie 3 : Mongo",
            "Partie 4 : Dashboard",
            "Partie 5 : Comparaison",
            "Partie 6 : Assistant requêtes IA",
        ],
    )

    render_partie_1_sqlite(tab1)
    render_partie_2_migration(tab2)
    render_partie_3_mongo(tab3)
    render_partie_4_streamlit(tab4)
    render_partie_5_comparaison(tab5)
    render_partie_6_ia(tab6)

if __name__ == "__main__":
    main()