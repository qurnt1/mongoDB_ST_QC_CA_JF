# =====================================================================
# LANCEMENT DE L'APPLICATION STREAMLIT
# À exécuter depuis le terminal :
#     python -m streamlit run app.py
# =====================================================================

# =====================================================================
# PARTIE 0 - IMPORTS ET CONFIGURATION GLOBALE
# =====================================================================

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
from dotenv import load_dotenv, set_key, find_dotenv

# Les modules suivants contiennent les requêtes SQL et MongoDB métier.
from data.sqlite.requetes_SQLite import executer_requetes_sql_module
from data.mongodb.requetes_MongoDB import QUERY_MONGO_FUNCS, aggregate_to_df

# =====================================================================
# PARTIE 1 - CONSTANTES DE CHEMINS ET PARAMÈTRES GÉNÉRAUX
# =====================================================================

# Dossier racine pour tous les fichiers locaux (db, JSON, CSV…)
DOSSIER_DATA = "data"
DOSSIER_SQLITE = "sqlite"

# Fichier principal de la base SQLite source
DB_FILE = os.path.join(DOSSIER_DATA, DOSSIER_SQLITE, "db", "paris2055.sqlite")

# Dossiers de sortie pour les résultats SQL (CSV), pour les collections Mongo (JSON)
# et pour les résultats de requêtes Mongo (CSV).
DOSSIER_CSV = "./data/sqlite/resultats_requetes_sqlite/"
DOSSIER_JSON = os.path.join(DOSSIER_DATA, "mongodb", "db")
DOSSIER_MONGO_CSV = os.path.join(
    DOSSIER_DATA,
    "mongodb",
    "resultats_requetes_mongodb",
)

# Chargement du fichier .env (clé Groq, URI MongoDB, etc.)
load_dotenv()
GROQ_API_KEY = os.getenv("GROQ_API_KEY")


def get_current_mongo_uri() -> str:
    """
    Retourne l'URI MongoDB actuellement utilisée par l'application.

    Ordre de priorité :
    1. Valeur stockée dans la session Streamlit (modifiée via la sidebar).
    2. Variable d'environnement MONGO_URI (fichier .env ou environnement système).
    3. Valeur par défaut locale "mongodb://127.0.0.1:27017/".
    """
    # 1. Priorité Session Streamlit
    if "mongo_uri" in st.session_state and st.session_state["mongo_uri"]:
        return st.session_state["mongo_uri"]

    # 2. Priorité Variable d'environnement (avec gestion chaîne vide)
    env_uri = os.getenv("MONGO_URI")
    if env_uri and env_uri.strip():  # Vérifie que ce n'est pas None et pas vide/espaces
        return env_uri

    # 3. Valeur par défaut locale
    return "mongodb://127.0.0.1:27017/"


# URI et nom de base MongoDB par défaut (peuvent être modifiés via l'IHM)
MONGO_URI = get_current_mongo_uri()
MONGO_DB_NAME = "Paris2055"

# =====================================================================
# PARTIE 2 - CONTEXTE DE SCHÉMA POUR L’ASSISTANT IA
# =====================================================================


def charger_prompt_context(filepath: str) -> str:
    """
    Charge le texte qui décrit le schéma métier (prompt système pour l'IA).

    Le contenu du fichier est utilisé comme contexte lors des appels à Groq,
    afin que le modèle sache comment sont structurées les collections MongoDB.

    Paramètres
    ----------
    filepath : str
        Chemin complet du fichier texte à lire.

    Retour
    ------
    str
        Contenu du fichier ou chaîne vide en cas de problème.
    """
    if not os.path.exists(filepath):
        print(f"⚠️ Fichier de contexte introuvable : {filepath}")
        return ""

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as exc:
        print(f"⚠️ Erreur lors de la lecture du contexte IA : {exc}")
        return ""


CHEMIN_PROMPT = os.path.join(DOSSIER_DATA, "context_prompt.txt")
SCHEMA_CONTEXT = charger_prompt_context(CHEMIN_PROMPT)

# =====================================================================
# PARTIE 3 - OUTILS GÉNÉRIQUES (SANS DÉPENDANCE À SQL/MONGO/UI)
# =====================================================================


def log_progress(
    current: int,
    total: int,
    prefix: str,
    log_fn: Callable[[str, bool], None],
    step_percent: Optional[int] = None,
) -> None:
    """
    Trace l'avancement d'un traitement long (migration, groupement, etc.).

    Le message est produit via une fonction de log externe (console,
    zone de texte Streamlit, etc.) afin de rester agnostique de l'interface.

    Paramètres
    ----------
    current : int
        Nombre d'éléments déjà traités.
    total : int
        Nombre total d'éléments à traiter.
    prefix : str
        Intitulé fonctionnel du traitement (ex. "Assemblage Lignes").
    log_fn : Callable[[str, bool], None]
        Fonction de log prenant en argument le message et un booléen
        indiquant s'il faut écraser la dernière ligne (mode temps réel).
    step_percent : int | None
        Pourcentage minimal entre deux logs (ex. 1 → log chaque 1%).
        Si None, aucun filtrage n'est appliqué.

    Remarque
    --------
    En cas de total nul, la fonction ne fait rien pour éviter la division par zéro.
    """
    if total == 0:
        return

    if step_percent is not None:
        step = max(int(total * (step_percent / 100)), 1)
        # On ne logge pas à chaque élément pour éviter de surcharger l'affichage.
        if current % step != 0 and current != total:
            return

    pct = (current / total) * 100
    message = f"    >> {prefix} : {current:,} / {total:,} ({pct:.1f}%)"
    log_fn(message, replace_last=True)


def console_log(message: str, replace_last: bool = False) -> None:
    """
    Fonction de log par défaut (console).

    Utilisée lorsque aucune fonction spécifique (Streamlit, fichier, etc.)
    n'est fournie. Le paramètre replace_last est ignoré mais conservé pour
    rester compatible avec les autres fonctions de log.
    """
    print(message)


def parse_geojson_geometry(geojson_str: Optional[str]) -> Optional[Dict]:
    """
    Interprète un texte JSON au format GeoJSON et en extrait la géométrie.

    La fonction traite deux cas principaux :
    - un objet de type géométrie (Point, Polygon, etc.) ;
    - un objet de type Feature contenant un champ 'geometry'.

    Paramètres
    ----------
    geojson_str : str | None
        Chaîne JSON représentant un objet GeoJSON ou une Feature.

    Retour
    ------
    dict | None
        Dictionnaire minimal sous la forme :
            {"type": ..., "coordinates": ...}
        ou None si le contenu est vide, invalide ou sans géométrie.
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

    if isinstance(obj, dict):
        # Cas d'une géométrie GeoJSON directe
        if "type" in obj and "coordinates" in obj:
            return {"type": obj["type"], "coordinates": obj["coordinates"]}

        # Cas d'une Feature qui encapsule la géométrie
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
    Propose une unité de mesure plausible à partir du type de capteur.

    Cette fonction est un filet de sécurité lorsque la colonne 'unite'
    n'est pas renseignée dans la table des mesures.

    Paramètres
    ----------
    type_capteur : str | None
        Libellé du capteur (ex. "Capteur Bruit", "Capteur CO2").

    Retour
    ------
    str | None
        Unité déduite :
            - "dB" pour le bruit,
            - "°C" pour la température,
            - "ppm" pour le CO2,
        ou None si aucun mot-clé pertinent n'est détecté.
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
# PARTIE 4 - ÉTAPE ETL : JSON / INSERTION MONGO / CONSTRUCTION DOCUMENTS
# =====================================================================


def sauvegarder_collection_json(
    nom_collection: str,
    data: List[Dict],
    log_fn: Callable[[str, bool], None],
) -> str:
    """
    Enregistre une liste de documents Python dans un fichier JSON.

    Le fichier est stocké dans DOSSIER_JSON et sert d'export intermédiaire
    entre la base relationnelle et MongoDB.

    Paramètres
    ----------
    nom_collection : str
        Nom logique de la collection (utilisé dans le nom du fichier).
    data : list[dict]
        Documents à sérialiser.
    log_fn : Callable[[str, bool], None]
        Fonction utilisée pour tracer l'avancement de l'écriture.

    Retour
    ------
    str
        Nom du fichier créé (sans le chemin complet).
    """
    file_name = f"Collection_{nom_collection}.json"
    full_path = os.path.join(DOSSIER_JSON, file_name)
    total = len(data)

    log_fn(
        f"   💾 [JSON] Écriture du fichier {file_name} ({total:,} documents)...",
        replace_last=False,
    )

    try:
        with open(full_path, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, ensure_ascii=False, default=str, indent=4)

        log_fn(
            f"   ✅ [JSON] Fichier généré ({total:,} documents).",
            replace_last=False,
        )
        return file_name
    except Exception as exc:
        log_fn(
            f"   💥 [ERREUR] Échec lors de la sauvegarde JSON : {exc}",
            replace_last=False,
        )
        raise


def insert_with_progress(
    collection,
    docs: List[Dict],
    label: str,
    batch_size: int = 25000,
    log_fn: Callable[[str, bool], None] = console_log,
) -> None:
    """
    Insère une liste de documents dans une collection MongoDB par paquets.

    Cette fonction découpe la liste en lots afin d'éviter de trop gros
    envois d'un coup vers MongoDB et trace l'avancement au fur et à mesure.

    Paramètres
    ----------
    collection :
        Collection MongoDB cible (par ex. db.lignes).
    docs : list[dict]
        Documents à insérer.
    label : str
        Libellé métier de la collection (utilisé dans les logs).
    batch_size : int
        Taille des lots d'insertion.
    log_fn : Callable[[str, bool], None]
        Fonction de log. Par défaut, console_log().
    """
    total = len(docs)
    if total == 0:
        return

    log_fn(
        f"   📤 [MONGO] Insertion de {total:,} documents dans '{label}'...",
        replace_last=False,
    )
    log_progress(0, total, "Insertion MongoDB", log_fn)

    inserted_count = 0

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
            step_percent=1,
        )

    log_fn(
        f"   ✨ [MONGO] Collection '{label}' entièrement insérée.\n",
        replace_last=False,
    )


def load_tables(
    conn: sqlite3.Connection,
    log_fn: Callable[[str, bool], None],
) -> Dict[str, pd.DataFrame]:
    """
    Charge en mémoire les tables nécessaires depuis la base SQLite.

    Les tables sont retournées dans un dictionnaire : le nom de table
    est utilisé comme clé, et le DataFrame correspondant comme valeur.

    Paramètres
    ----------
    conn : sqlite3.Connection
        Connexion ouverte vers la base SQLite.
    log_fn : Callable[[str, bool], None]
        Fonction de log indiquant l'avancement du chargement.

    Retour
    ------
    dict[str, pandas.DataFrame]
        Dictionnaire nom de table → DataFrame.
    """
    log_fn("📥 [SQL] Chargement des tables SQLite...", replace_last=False)
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
    Construit la collection documentaire 'lignes' à partir des tables SQLite.

    Chaque document 'ligne' regroupe :
    - les informations de la ligne (nom, type, fréquentation) ;
    - la liste des arrêts rattachés à la ligne ;
    - les horaires (avec véhicules rattachés) regroupés par arrêt ;
    - des informations de trafic et d'incidents ;
    - des caches pré-calculés (chauffeurs, véhicules, statistiques de trafic,
      moyenne de CO2 par ligne).

    Paramètres
    ----------
    tables : dict[str, pandas.DataFrame]
        Tables relationnelles déjà chargées via load_tables().
    log_fn : Callable[[str, bool], None]
        Fonction permettant de tracer chaque grande étape.

    Retour
    ------
    list[dict]
        Liste de documents prêts à être insérés dans db.lignes.
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

    # Calcul des moyennes de CO2 par ligne à partir des capteurs situés aux arrêts.
    co2_by_ligne: Dict[int, float] = {}
    if not df_cap.empty and not df_mes.empty:
        df_full = df_cap.merge(df_mes, on="id_capteur")
        df_co2 = df_full[df_full["type_capteur"] == "CO2"]
        df_co2_ligne = df_co2.merge(df_a[["id_arret", "id_ligne"]], on="id_arret")
        if not df_co2_ligne.empty:
            co2_by_ligne = (
                df_co2_ligne.groupby("id_ligne")["valeur"].mean().to_dict()
            )

    # Préparation d'un cache par ligne des chauffeurs associés (optimisation requête K).
    log_fn("⚡ Pré-calcul des chauffeurs par ligne...", replace_last=False)
    chauffeurs_cache_by_ligne: Dict[int, List[Dict]] = {}
    if not df_v.empty and not df_c.empty:
        df_vc = df_v.merge(df_c, on="id_chauffeur", how="inner")
        if "id_ligne" in df_vc.columns:
            for id_ligne, group in df_vc.groupby("id_ligne"):
                if pd.isna(id_ligne):
                    continue
                c_list = []
                for _, row in group.iterrows():
                    if pd.notnull(row.get("nom")):
                        c_list.append(
                            {
                                "id_chauffeur": int(row["id_chauffeur"]),
                                "nom_chauffeur": row["nom"],
                            }
                        )
                if c_list:
                    chauffeurs_cache_by_ligne[int(id_ligne)] = c_list

    # Préparation d'un cache des véhicules par ligne (utilisé pour les requêtes D et L).
    log_fn("⚡ Pré-calcul des véhicules par ligne...", replace_last=False)
    vehicules_cache_by_ligne: Dict[int, List[Dict]] = {}
    if not df_v.empty:
        for id_ligne, group in df_v.groupby("id_ligne"):
            if pd.isna(id_ligne):
                continue
            v_list = []
            for _, row in group.iterrows():
                if pd.notnull(row.get("immatriculation")):
                    v_doc: Dict[str, object] = {
                        "id_vehicule": int(row["id_vehicule"]),
                        "immatriculation": row["immatriculation"],
                    }
                    if pd.notnull(row.get("type_vehicule")):
                        v_doc["type_vehicule"] = row["type_vehicule"]
                    v_list.append(v_doc)
            if v_list:
                vehicules_cache_by_ligne[int(id_ligne)] = v_list

    # Préparation des informations de trafic et incidents par ligne.
    log_fn("⚡ Pré-calcul des statistiques de trafic...", replace_last=False)
    stats_trafic_by_ligne: Dict[int, Dict] = {}
    trafic_by_ligne: Dict[int, List[Dict]] = {}
    incidents_by_trafic: Dict[int, List[Dict]] = {}

    if not df_i.empty:
        for id_trafic, group in df_i.groupby("id_trafic"):
            incidents_by_trafic[id_trafic] = group[
                ["id_incident", "description", "gravite"]
            ].to_dict("records")

    if not df_t.empty:
        stats_group = df_t.groupby("id_ligne")["retard_minutes"].agg(
            ["sum", "count", "mean"]
        )

        for id_ligne, row_stat in stats_group.iterrows():
            stats_trafic_by_ligne[int(id_ligne)] = {
                "total_retard": float(row_stat["sum"]),
                "nb_trajets": int(row_stat["count"]),
                "moyenne_precalc": float(row_stat["mean"]),
            }

        for _, row in df_t.iterrows():
            if pd.isna(row["id_ligne"]):
                continue
            tdoc: Dict[str, object] = {"id_trafic": int(row["id_trafic"])}
            if pd.notnull(row.get("horodatage")):
                tdoc["horodatage"] = row["horodatage"]
            if pd.notnull(row.get("retard_minutes")):
                tdoc["retard_minutes"] = int(row["retard_minutes"])
            if row["id_trafic"] in incidents_by_trafic:
                tdoc["incidents"] = incidents_by_trafic[row["id_trafic"]]
            trafic_by_ligne.setdefault(int(row["id_ligne"]), []).append(tdoc)

    # Préparation des quartiers par arrêt (rattachement géographique).
    quartiers_by_arret: Dict[int, List[Dict]] = {}
    if not df_aq.empty:
        tmp = df_aq.merge(
            df_q[["id_quartier", "nom"]].rename(columns={"nom": "nom_quartier"}),
            on="id_quartier",
            how="left",
        )
        for id_arret, group in tmp.groupby("id_arret"):
            subset = group[["id_quartier", "nom_quartier"]].drop_duplicates(
                "id_quartier"
            )
            quartiers_by_arret[id_arret] = [
                {"id_quartier": int(row["id_quartier"]), "nom": row["nom_quartier"]}
                for _, row in subset.iterrows()
                if pd.notnull(row["id_quartier"])
            ]

    # Liste d'identifiants de capteurs attachés à chaque arrêt.
    capteurs_ids_by_arret: Dict[int, List[int]] = {}
    if not df_cap.empty:
        for id_arret, group in df_cap.groupby("id_arret"):
            capteurs_ids_by_arret[id_arret] = [
                int(v) for v in group["id_capteur"].dropna().unique().tolist()
            ]

    # Construction des horaires par arrêt, en y rattachant les véhicules associés.
    horaires_by_arret: Dict[int, List[Dict]] = {}
    if not df_h.empty:
        df_v_clean = df_v.rename(columns={"id_ligne": "id_ligne_officielle"})
        df_h_full = df_h.merge(
            df_v_clean, on="id_vehicule", how="left", suffixes=("", "_vehicule")
        )

        if "heure_prevue" in df_h_full.columns:
            df_h_full["heure_prevue"] = pd.to_datetime(
                df_h_full["heure_prevue"],
                errors="coerce",
            )

        total_rows = len(df_h_full)
        log_progress(0, total_rows, "Groupement Horaires/Arrêts", log_fn)

        for idx, row in enumerate(df_h_full.itertuples(index=False), start=1):
            if pd.isna(row.id_arret):
                continue

            vehicule: Dict[str, object] = {}
            if getattr(row, "id_vehicule", None) and not pd.isna(row.id_vehicule):
                vehicule["id_vehicule"] = int(row.id_vehicule)
                if hasattr(row, "type_vehicule"):
                    vehicule["type_vehicule"] = row.type_vehicule
                if hasattr(row, "immatriculation"):
                    vehicule["immatriculation"] = row.immatriculation
                if hasattr(row, "id_ligne_officielle") and pd.notnull(
                    row.id_ligne_officielle
                ):
                    vehicule["id_ligne_officielle"] = int(row.id_ligne_officielle)

            horaire: Dict[str, object] = {}
            if hasattr(row, "heure_prevue") and pd.notnull(row.heure_prevue):
                horaire["heure_prevue"] = row.heure_prevue.to_pydatetime()
            if hasattr(row, "passagers_estimes") and pd.notnull(row.passagers_estimes):
                horaire["passagers_estimes"] = int(row.passagers_estimes)
            if vehicule:
                horaire["vehicule"] = vehicule

            horaires_by_arret.setdefault(int(row.id_arret), []).append(horaire)
            if idx % 10000 == 0:
                log_progress(idx, total_rows, "Groupement Horaires/Arrêts", log_fn)

        log_fn("", replace_last=False)

    # Regroupement final des arrêts par ligne avec toutes les informations
    # associées : quartiers, horaires, capteurs.
    arrets_by_ligne: Dict[int, List[Dict]] = {}
    for _, row in df_a.iterrows():
        if pd.isna(row["id_ligne"]) or pd.isna(row["id_arret"]):
            continue
        id_ligne = int(row["id_ligne"])
        id_arret = int(row["id_arret"])

        adoc: Dict[str, object] = {
            "id_arret": id_arret,
            "nom": row["nom"],
        }

        if "latitude" in row and pd.notnull(row["latitude"]):
            adoc["latitude"] = float(row["latitude"])
        if "longitude" in row and pd.notnull(row["longitude"]):
            adoc["longitude"] = float(row["longitude"])

        if id_arret in quartiers_by_arret:
            adoc["quartiers"] = quartiers_by_arret[id_arret]
        if id_arret in horaires_by_arret:
            adoc["horaires"] = horaires_by_arret[id_arret]
        if id_arret in capteurs_ids_by_arret:
            adoc["capteurs_ids"] = capteurs_ids_by_arret[id_arret]

        arrets_by_ligne.setdefault(id_ligne, []).append(adoc)

    # Assemblage final des documents 'lignes' (vue agrégée par ligne de transport).
    docs: List[Dict] = []
    total = len(df_l)
    log_progress(0, total, "Assemblage Lignes", log_fn)

    for idx, (_, row) in enumerate(df_l.iterrows(), start=1):
        if pd.isna(row["id_ligne"]):
            continue

        id_l = int(row["id_ligne"])
        doc: Dict[str, object] = {
            "id_ligne": id_l,
            "nom_ligne": row.get("nom_ligne"),
            "type": row.get("type"),
        }

        if id_l in co2_by_ligne:
            doc["co2_moyen_ligne"] = co2_by_ligne[id_l]
        if id_l in chauffeurs_cache_by_ligne:
            doc["chauffeurs_cache"] = chauffeurs_cache_by_ligne[id_l]
        if id_l in vehicules_cache_by_ligne:
            doc["vehicules_cache"] = vehicules_cache_by_ligne[id_l]
        if id_l in stats_trafic_by_ligne:
            doc["stats_trafic"] = stats_trafic_by_ligne[id_l]

        if id_l in arrets_by_ligne:
            doc["arrets"] = arrets_by_ligne[id_l]
        if id_l in trafic_by_ligne:
            doc["trafic"] = trafic_by_ligne[id_l]
        if pd.notnull(row.get("frequentation_moyenne")):
            doc["frequentation_moyenne"] = float(row["frequentation_moyenne"])

        docs.append(doc)
        if idx % 100 == 0:
            log_progress(idx, total, "Assemblage Lignes", log_fn)

    return docs


def build_quartiers_docs(
    tables: Dict[str, pd.DataFrame],
    log_fn: Callable[[str, bool], None],
) -> List[Dict]:
    """
    Construit la collection documentaire 'quartiers' à partir des tables SQLite.

    Chaque document 'quartier' regroupe :
    - son identifiant et son nom ;
    - éventuellement sa géométrie (polygone GeoJSON) ;
    - la liste des arrêts situés dans ce quartier, avec la ligne associée.

    Paramètres
    ----------
    tables : dict[str, pandas.DataFrame]
        Tables chargées via load_tables().
    log_fn : Callable[[str, bool], None]
        Fonction de log utilisée pour suivre l'avancement des traitements.

    Retour
    ------
    list[dict]
        Liste de documents prêts pour la collection db.quartiers.
    """
    df_q = tables["Quartier"]
    df_aq = tables["ArretQuartier"]
    df_a = tables["Arret"]
    df_l = tables["Ligne"]

    arrets_by_quartier: Dict[int, List[Dict]] = {}
    if not df_aq.empty:
        log_fn(
            "🔧 Groupement des arrêts par quartier...",
            replace_last=False,
        )
        df_aq_full = df_aq.merge(
            df_a[
                [
                    "id_arret",
                    "nom",
                    "id_ligne",
                    "latitude",
                    "longitude",
                ]
            ].rename(columns={"nom": "nom_arret"}),
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
                    "latitude",
                    "longitude",
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

                # Ligne associée
                if pd.notnull(row.get("id_ligne")):
                    stop_doc["id_ligne"] = int(row["id_ligne"])
                if pd.notnull(row.get("nom_ligne")):
                    stop_doc["nom_ligne"] = row["nom_ligne"]

                # Coordonnées de l'arrêt
                if pd.notnull(row.get("latitude")) and pd.notnull(row.get("longitude")):
                    stop_doc["latitude"] = float(row["latitude"])
                    stop_doc["longitude"] = float(row["longitude"])

                stops.append(stop_doc)


            arrets_by_quartier[id_quartier] = stops
            log_progress(
                idx,
                nb_groups,
                "Groupement Arrêts/Quartiers",
                log_fn,
                step_percent=1,
            )

        log_fn("", replace_last=False)

    docs: List[Dict] = []
    total_quartiers = len(df_q)
    label_final = "Construction des documents quartiers"
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
    Construit la collection documentaire 'capteurs' à partir des tables SQLite.

    Chaque document de la collection inclut :
    - les informations du capteur (type, position) ;
    - l'arrêt auquel il est rattaché (avec la ligne et son nom) ;
    - l'historique des mesures (valeur, date, unité).

    Paramètres
    ----------
    tables : dict[str, pandas.DataFrame]
        Tables préalablement chargées.
    log_fn : Callable[[str, bool], None]
        Fonction de journalisation des différentes étapes.

    Retour
    ------
    list[dict]
        Documents prêts à être insérés dans db.capteurs.
    """
    df_capteur = tables["Capteur"]
    df_mesure = tables["Mesure"]
    df_arret = tables["Arret"]
    df_ligne = tables["Ligne"]

    # Préparation des informations d'arrêt associées à chaque capteur.
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

    # Mémorisation du type de capteur pour déduire l'unité si besoin.
    type_capteur_by_id: Dict[int, Optional[str]] = {}
    for _, row in df_capteur.iterrows():
        if pd.isna(row["id_capteur"]):
            continue

        capteur_id = int(row["id_capteur"])
        type_capteur_by_id[capteur_id] = row.get("type_capteur")

    # Groupement des mesures par capteur avec homogénéisation de l'unité.
    mesures_by_capteur: Dict[int, List[Dict]] = {}
    if not df_mesure.empty:
        log_fn(
            "🔧 Groupement des mesures par capteur...",
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
                step_percent=1,
            )

        log_fn("", replace_last=False)

    # Assemblage final des documents capteurs.
    docs: List[Dict] = []
    total_capteurs = len(df_capteur)
    label_final = "Construction des documents capteurs"
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


def creer_index_mongodb(
    db: pymongo.database.Database,
    log_fn: Callable[[str, bool], None],
) -> None:
    """
    Crée les index nécessaires sur les collections MongoDB pour accélérer
    les requêtes métier (lignes, quartiers, capteurs).

    Paramètres
    ----------
    db : pymongo.database.Database
        Base MongoDB cible.
    log_fn : Callable[[str, bool], None]
        Fonction de log pour informer de la progression.
    """
    log_fn("[INDEX] Création des index sur MongoDB...", replace_last=False)
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

        log_fn("[INDEX] Index créés avec succès.", replace_last=False)
    except Exception as exc:
        log_fn(
            f"[INDEX] Erreur lors de la création des index : {exc}",
            replace_last=False,
        )


def migrer_sqlite_vers_mongo(
    log_fn_raw: Callable[[str, bool], None],
) -> None:
    """
    Chaîne complète de migration depuis SQLite vers MongoDB.

    Étapes appliquées :
    1. Connexion à MongoDB et nettoyage des anciennes collections.
    2. Ouverture du fichier SQLite et chargement des tables.
    3. Construction des collections documentaires (lignes, quartiers, capteurs).
    4. Sauvegarde intermédiaire en fichiers JSON.
    5. Insertion en masse des documents dans MongoDB avec suivi de progression.
    6. Création des index nécessaires pour les requêtes métier.

    Paramètres
    ----------
    log_fn_raw : Callable[[str, bool], None]
        Fonction de log fournie par l'interface (peut ou non accepter
        l'argument 'replace_last').
    """

    def secure_log(message: str, replace_last: bool = False) -> None:
        """
        Adapte la fonction de log fournie par l'appelant.

        Si la fonction appelée ne supporte pas le paramètre 'replace_last',
        on bascule sur un simple print afin d'éviter les erreurs.
        """
        try:
            log_fn_raw(message, replace_last=replace_last)
        except TypeError:
            print(message)

    def process_step(
        label: str,
        build_func: Callable[
            [Dict[str, pd.DataFrame], Callable[[str, bool], None]],
            List[Dict],
        ],
        tables: Dict[str, pd.DataFrame],
    ) -> None:
        """
        Enchaîne la construction des documents, la sauvegarde JSON
        et l'insertion en base pour une collection donnée.

        Paramètres
        ----------
        label : str
            Nom de la collection MongoDB et du fichier JSON.
        build_func : Callable
            Fonction de construction des documents à partir des tables SQLite.
        tables : dict[str, pandas.DataFrame]
            Ensemble des tables chargées depuis SQLite.
        """
        secure_log(
            f"\n🔹 --- TRAITEMENT COLLECTION : {label.upper()} ---",
            replace_last=False,
        )

        secure_log(
            "   ⚙️  Construction du modèle documentaire...",
            replace_last=False,
        )
        documents = build_func(tables, secure_log)
        secure_log(
            f"   👌  Construction terminée : {len(documents):,} documents générés.",
            replace_last=False,
        )

        sauvegarder_collection_json(label, documents, secure_log)

        insert_with_progress(
            db[label],
            documents,
            label,
            log_fn=secure_log,
        )

        del documents  # Libération mémoire explicite pour cette collection

    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]

    secure_log("🚀 DÉBUT DE LA MIGRATION (ETL)", replace_last=False)
    secure_log(
        "   Mode : SQLite → Documents JSON → MongoDB",
        replace_last=False,
    )

    try:
        secure_log(
            "\n🧹 [INIT] Nettoyage des collections existantes...",
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
                    continue

        secure_log(
            f"   🗑️  {dropped_count} anciennes collections supprimées.",
            replace_last=False,
        )

        if not os.path.exists(DB_FILE):
            raise FileNotFoundError(f"Base SQLite introuvable : {DB_FILE}")

        sqlite_conn = sqlite3.connect(DB_FILE)
        try:
            tables = load_tables(sqlite_conn, secure_log)
        finally:
            sqlite_conn.close()

        process_step("lignes", build_lignes_docs, tables)
        process_step("quartiers", build_quartiers_docs, tables)
        process_step("capteurs", build_capteurs_docs, tables)

        secure_log(
            "\n🔎 [INDEX] Mise en place des index MongoDB...",
            replace_last=False,
        )
        creer_index_mongodb(db, secure_log)

        secure_log(
            "\n🎉 --- MIGRATION TERMINÉE SANS ERREUR ---",
            replace_last=False,
        )

    except Exception as exc:
        secure_log(f"\n💥 [ERREUR CRITIQUE MIGRATION] : {exc}", replace_last=False)
    finally:
        client.close()

# =====================================================================
# PARTIE 5 - SQL MÉTIER : OBJECTIFS ET EXÉCUTION DES REQUÊTES
# =====================================================================

# Description textuelle des objectifs de chaque requête métier (A → N)
REQUETES_OBJECTIFS: Dict[str, str] = {
    "A": (
        "Moyenne des retards (minutes) pour chaque ligne, triée par ordre décroissant."
    ),
    "B": (
        "Estimation du nombre moyen de passagers transportés par jour pour chaque ligne."
    ),
    "C": (
        "Taux d'incidents par ligne (en pourcentage des trajets ayant un incident)."
    ),
    "D": (
        "Moyenne des émissions de CO2 associée aux véhicules, triée par ordre décroissant."
    ),
    "E": (
        "Top 5 des quartiers ayant la moyenne de niveau de bruit (dB) la plus élevée."
    ),
    "F": (
        "Lignes ayant des retards > 10 minutes mais aucun incident (logique type EXCEPT)."
    ),
    "G": (
        "Taux de ponctualité global (pourcentage de trajets avec retard nul)."
    ),
    "H": (
        "Nombre d'arrêts desservis par chaque quartier, trié par ordre décroissant."
    ),
    "I": (
        "Corrélation entre la moyenne des retards et la moyenne des émissions de CO2 par ligne."
    ),
    "J": (
        "Moyenne de température mesurée aux arrêts pour chaque ligne de transport."
    ),
    "K": (
        "Moyenne des retards (minutes) pour chaque chauffeur sur l'ensemble de ses trajets."
    ),
    "L": (
        "Pour les lignes de bus, pourcentage de véhicules électriques dans la flotte."
    ),
    "M": (
        "Classement des capteurs de CO2 par niveau de pollution (Élevé / Moyen / Faible)."
    ),
    "N": (
        "Segmentation des lignes par niveau de fréquentation (Haute / Moyenne / Basse)."
    ),
}


@st.cache_data(show_spinner=False)
def executer_toutes_les_requetes() -> Dict[str, pd.DataFrame]:
    """
    Appelle le module externe contenant les requêtes SQL et retourne
    les résultats sous forme de dictionnaire.

    La fonction est mise en cache par Streamlit pour éviter des recalculs
    coûteux à chaque interaction de l'utilisateur.

    Retour
    ------
    dict[str, pandas.DataFrame]
        Résultats pour chaque requête A → N.
    """
    return executer_requetes_sql_module()

# =====================================================================
# PARTIE 6 - MONGODB MÉTIER : EXÉCUTION DES REQUÊTES A → N
# =====================================================================


@st.cache_data(show_spinner=False)
def executer_toutes_les_requetes_mongo(
    mongo_uri: str,
    db_name: str,
) -> Dict[str, pd.DataFrame]:
    """
    Exécute l'ensemble des requêtes MongoDB métier définies dans QUERY_MONGO_FUNCS.

    Pour chaque code de requête (A → N), la fonction correspondante est appelée
    avec la base MongoDB cible puis convertit son résultat en DataFrame.

    Paramètres
    ----------
    mongo_uri : str
        URI du serveur MongoDB à utiliser.
    db_name : str
        Nom de la base de données MongoDB à interroger.

    Retour
    ------
    dict[str, pandas.DataFrame]
        Dictionnaire code de requête → DataFrame résultat.
    """
    client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=2000)
    resultats: Dict[str, pd.DataFrame] = {}

    try:
        client.admin.command("ping")
        if db_name not in client.list_database_names():
            error_df = pd.DataFrame(
                [{"erreur": f"Base '{db_name}' inexistante."}]
            )
            return {code: error_df for code in QUERY_MONGO_FUNCS.keys()}

        db = client[db_name]

        for code, func in QUERY_MONGO_FUNCS.items():
            try:
                df = func(db)
            except Exception as exc:
                df = pd.DataFrame(
                    [{"erreur": f"Erreur lors de la requête MongoDB : {str(exc)}"}]
                )
            resultats[code] = df

    except Exception as exc:
        err_msg = f"Impossible de se connecter à MongoDB : {str(exc)}"
        error_df = pd.DataFrame([{"erreur": err_msg}])
        return {code: error_df for code in QUERY_MONGO_FUNCS.keys()}
    finally:
        client.close()

    return resultats

# =====================================================================
# PARTIE 7 - PERSISTANCE CSV (SQL & MONGODB)
# =====================================================================


def forcer_ecriture_csv_sql(
    resultats: Dict[str, pd.DataFrame],
    log_fn: Callable[[str, bool], None] = console_log,
) -> None:
    """
    Sauvegarde sur disque les résultats des requêtes SQL au format CSV.

    Chaque requête A → N est écrite dans un fichier distinct
    dans le dossier DOSSIER_CSV.

    Paramètres
    ----------
    resultats : dict[str, pandas.DataFrame]
        Résultats produits par executer_toutes_les_requetes().
    log_fn : Callable[[str, bool], None]
        Fonction de log à utiliser (console par défaut).
    """
    os.makedirs(DOSSIER_CSV, exist_ok=True)
    log_fn("💾 Sauvegarde des résultats SQL au format CSV...", False)
    for code, df in resultats.items():
        nom_fichier = f"resultat_req_{code.lower()}.csv"
        full_path = os.path.join(DOSSIER_CSV, nom_fichier)
        try:
            df.to_csv(full_path, index=False, encoding="utf-8-sig")
        except Exception as exc:
            log_fn(f"Erreur lors de l'écriture de {nom_fichier} : {exc}", False)
    log_fn("✅ Fichiers CSV SQL générés.", False)


def forcer_ecriture_csv_mongo(
    resultats: Dict[str, pd.DataFrame],
    log_fn: Callable[[str, bool], None] = console_log,
) -> None:
    """
    Sauvegarde sur disque les résultats des requêtes MongoDB au format CSV.

    Paramètres
    ----------
    resultats : dict[str, pandas.DataFrame]
        Dictionnaire code → DataFrame, produit par executer_toutes_les_requetes_mongo().
    log_fn : Callable[[str, bool], None]
        Fonction de log à utiliser (console par défaut).
    """
    os.makedirs(DOSSIER_MONGO_CSV, exist_ok=True)
    log_fn("💾 Sauvegarde des résultats MongoDB au format CSV...", False)
    for code, df in resultats.items():
        nom_fichier = f"resultat_req_{code.lower()}.csv"
        full_path = os.path.join(DOSSIER_MONGO_CSV, nom_fichier)
        try:
            df.to_csv(full_path, index=False, encoding="utf-8-sig")
        except Exception as exc:
            log_fn(f"Erreur lors de l'écriture de {nom_fichier} : {exc}", False)
    log_fn("✅ Fichiers CSV MongoDB générés.", False)


def tenter_chargement_depuis_csv(dossier_cible: str) -> Dict[str, pd.DataFrame]:
    """
    Tente de reconstituer les résultats des requêtes à partir de fichiers CSV.

    Cette fonction est utilisée au démarrage de l'application pour recharger
    l'état des requêtes SQL ou MongoDB (en fonction du dossier passé).

    Paramètres
    ----------
    dossier_cible : str
        Dossier dans lequel chercher les fichiers CSV (SQL ou Mongo).

    Retour
    ------
    dict[str, pandas.DataFrame]
        Dictionnaire des DataFrames rechargés, indexé par code de requête.
    """
    resultats_charges: Dict[str, pd.DataFrame] = {}

    for code in REQUETES_OBJECTIFS.keys():
        nom_fichier = f"resultat_req_{code.lower()}.csv"
        chemin_complet = os.path.join(dossier_cible, nom_fichier)

        if os.path.exists(chemin_complet):
            try:
                df = pd.read_csv(chemin_complet)
                resultats_charges[code] = df
            except Exception:
                # Fichier corrompu ou illisible : on ignore simplement.
                pass

    return resultats_charges

# =====================================================================
# PARTIE 8 - CRÉATION DES DOSSIERS ET ÉTAT GLOBAL STREAMLIT
# =====================================================================

# Les dossiers indispensables au fonctionnement de l’application
# sont créés dès le chargement du module.
os.makedirs(DOSSIER_DATA, exist_ok=True)
os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
os.makedirs(DOSSIER_CSV, exist_ok=True)
os.makedirs(DOSSIER_JSON, exist_ok=True)
os.makedirs(DOSSIER_MONGO_CSV, exist_ok=True)


def init_session_state() -> None:
    """
    Prépare les variables stockées dans st.session_state.

    Cette étape garantit que l'application peut fonctionner même lors
    du premier lancement, sans qu'aucun calcul n'ait encore été effectué.
    """
    if "mongo_uri" not in st.session_state:
        st.session_state["mongo_uri"] = os.getenv(
            "MONGO_URI",
            "mongodb://127.0.0.1:27017/",
        )

    if st.session_state.get("initialized", False):
        return

    if "groq_api_key" not in st.session_state:
        st.session_state["groq_api_key"] = os.getenv("GROQ_API_KEY", "")

    st.session_state["requetes_objectifs"] = REQUETES_OBJECTIFS

    # Rechargement éventuel des résultats SQL depuis les CSV.
    if "resultats_sql" not in st.session_state:
        data_sql = tenter_chargement_depuis_csv(DOSSIER_CSV)
        if data_sql:
            st.session_state["resultats_sql"] = data_sql
            st.session_state["queries_sql_executed"] = True
            console_log("✅ Résultats SQL restaurés depuis les fichiers CSV.")
        else:
            st.session_state["resultats_sql"] = {}
            st.session_state["queries_sql_executed"] = False

    # Rechargement éventuel des résultats MongoDB depuis les CSV.
    if "resultats_mongo" not in st.session_state:
        data_mongo = tenter_chargement_depuis_csv(DOSSIER_MONGO_CSV)
        if data_mongo:
            st.session_state["resultats_mongo"] = data_mongo
            st.session_state["queries_mongo_executed"] = True
            console_log("✅ Résultats MongoDB restaurés depuis les fichiers CSV.")
        else:
            st.session_state["resultats_mongo"] = {}
            st.session_state["queries_mongo_executed"] = False

    # Variables de suivi de la migration et de l'assistant IA.
    st.session_state["migration_logs"] = []
    st.session_state["migration_done_msg"] = ""
    st.session_state["migration_running"] = False
    st.session_state["ai_json_response"] = None
    st.session_state["ai_question_text_value"] = ""

    st.session_state["initialized"] = True

# =====================================================================
# PARTIE 9 - CONTRÔLE DE LA CONNECTIVITÉ MONGODB
# =====================================================================


def check_connexion_details() -> tuple[bool, bool]:
    """
    Vérifie l'état du serveur MongoDB et la présence de la base Paris2055.

    Retour
    ------
    (bool, bool)
        - premier booléen : True si le serveur répond au ping,
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
        server_ok = False
        db_ok = False
    finally:
        if client is not None:
            client.close()

    return server_ok, db_ok

# =====================================================================
# PARTIE 10 - JOURNALISATION DE LA MIGRATION
# =====================================================================

# Placeholder graphique pour l'affichage des logs dans Streamlit
MIGRATION_LOG_PLACEHOLDER: Optional[st.delta_generator.DeltaGenerator] = None

# Nombre maximal de lignes conservées pour l'affichage des journaux
MAX_LOG_LINES = 300


def streamlit_migration_log(message: str, replace_last: bool = False) -> None:
    """
    Ajoute une ligne de journal dans l'historique de migration et
    met à jour la zone d'affichage associée dans l'interface.

    Paramètres
    ----------
    message : str
        Texte du log à afficher.
    replace_last : bool
        Si True, la dernière ligne est remplacée par ce message.
        Sinon, une nouvelle ligne est ajoutée à la fin.
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

# =====================================================================
# PARTIE 11 - UI STREAMLIT : PARTIES 1 À 5 (SQL / MIGRATION / MONGO / DASH / COMPARAISON)
# =====================================================================


def render_partie_1_sqlite(tab) -> None:
    """
    Affiche l'onglet "Partie 1 : Requêtes SQLite".

    L'utilisateur peut :
    - exécuter l'ensemble des requêtes SQL,
    - sauvegarder les résultats au format CSV,
    - consulter le détail des résultats par requête.
    """
    with tab:
        st.subheader("Partie 1 : Requêtes SQLite")

        status_text = (
            "Données chargées."
            if st.session_state["queries_sql_executed"]
            else "Données non chargées."
        )
        st.write(status_text)

        if st.button("Executer & Sauvegarder CSV", key="btn_sql_run"):
            with st.spinner("Calcul des requêtes SQL et enregistrement des CSV..."):
                res = executer_toutes_les_requetes()
                forcer_ecriture_csv_sql(res)
                st.session_state["resultats_sql"] = res
                st.session_state["queries_sql_executed"] = True
                st.success("✅ Calculs terminés et CSV mis à jour.")

        st.markdown("---")

        if not st.session_state["queries_sql_executed"]:
            st.info("Cliquez sur le bouton ci-dessus pour lancer les requêtes SQL.")
            return

        st.markdown("### Résultats détaillés des requêtes SQLite")

        for code, objectif in st.session_state["requetes_objectifs"].items():
            df = st.session_state["resultats_sql"].get(code)
            with st.expander(f"Requête {code} – {objectif}", expanded=False):
                st.markdown(f"**Objectif :** {objectif}")
                if df is None:
                    st.warning("Aucun résultat disponible.")
                elif df.empty:
                    st.info("Résultat vide (aucune ligne retournée).")
                else:
                    st.dataframe(
                        df.style.set_properties(**{"text-align": "left"}),
                        width="stretch",
                    )


def render_partie_2_migration(tab) -> None:
    """
    Affiche l'onglet "Partie 2 : Migration vers MongoDB".

    Cet écran permet :
    - de lancer la migration complète depuis SQLite vers MongoDB,
    - de suivre les logs en temps réel,
    - de visualiser l'historique de la dernière migration effectuée.
    """
    global MIGRATION_LOG_PLACEHOLDER

    with tab:
        st.subheader("Partie 2 : Migration vers MongoDB")

        server_ok, _ = check_connexion_details()

        if not server_ok:
            st.error(
                "🚫 Impossible de lancer la migration : le serveur MongoDB semble injoignable. "
                "Vérifiez l'URI dans la barre latérale."
            )
            st.button("Lancer Migration", disabled=True, key="btn_mig_disabled")
            return

        st.caption(
            "Cette étape lit la base SQLite, génère les collections documentaires "
            "et les insère dans MongoDB."
        )

        def start_migration_callback() -> None:
            """
            Prépare une nouvelle migration en réinitialisant l'état de session
            lié aux logs et au message de fin.
            """
            st.session_state["migration_running"] = True
            st.session_state["migration_logs"] = []
            st.session_state["migration_done_msg"] = ""

        col_btn, col_status = st.columns([1, 3], gap="small")

        with col_btn:
            st.button(
                "Lancer Migration",
                key="btn_migration",
                width="content",
                on_click=start_migration_callback,
                disabled=st.session_state.get("migration_running", False),
            )

        MIGRATION_LOG_PLACEHOLDER = st.empty()

        if st.session_state.get("migration_running", False):
            MIGRATION_LOG_PLACEHOLDER.code(
                "Initialisation de la migration...", language="text"
            )

            with col_status:
                with st.spinner("Migration en cours, merci de patienter..."):
                    migrer_sqlite_vers_mongo(log_fn_raw=streamlit_migration_log)

            st.session_state["migration_done_msg"] = "Migration terminée avec succès !"
            st.session_state["migration_running"] = False

            st.toast("Migration terminée 🎉", icon="🎉")
            time.sleep(1)
            st.rerun()

        if st.session_state.get("migration_done_msg"):
            with col_status:
                st.success(st.session_state["migration_done_msg"])

        logs: List[str] = st.session_state.get("migration_logs", [])
        if logs and not st.session_state.get("migration_running", False):
            log_text = "\n".join(logs[-MAX_LOG_LINES:])
            MIGRATION_LOG_PLACEHOLDER.text_area(
                "Journal de migration (dernier run)",
                value=log_text,
                height=400,
                key="final_log_view",
            )


def render_partie_3_mongo(tab) -> None:
    """
    Affiche l'onglet "Partie 3 : Requêtes MongoDB".

    L'utilisateur peut :
    - lancer les requêtes A → N sur la base MongoDB,
    - sauvegarder les résultats au format CSV,
    - consulter les résultats par requête.
    """
    with tab:
        st.subheader("Partie 3 : Requêtes MongoDB")

        server_ok, db_ok = check_connexion_details()

        if not server_ok:
            st.error("❌ Serveur MongoDB introuvable. Vérifiez l'URI dans la configuration.")
            st.info("💡 Astuce : si vous utilisez Docker, assurez-vous que le conteneur MongoDB est démarré.")
            return

        if not db_ok:
            st.warning(f"⚠️ La base '{MONGO_DB_NAME}' n'existe pas encore.")
            st.info("👉 Rendez-vous dans l'onglet **Partie 2 : Migration** pour créer la base de données.")

        btn_disabled = not (server_ok and db_ok)

        if st.button("Executer & Sauvegarder CSV Mongo", key="btn_mongo_run", disabled=btn_disabled):
            with st.spinner("Exécution des requêtes MongoDB et écriture des CSV..."):
                current_uri = st.session_state.get("mongo_uri", MONGO_URI)
                res = executer_toutes_les_requetes_mongo(current_uri, MONGO_DB_NAME)
                forcer_ecriture_csv_mongo(res)
                st.session_state["resultats_mongo"] = res
                st.session_state["queries_mongo_executed"] = True
                st.success("✅ Requêtes MongoDB exécutées et CSV enregistrés.")

        if not st.session_state.get("queries_mongo_executed"):
            st.info("Cliquez sur le bouton ci-dessus pour lancer les requêtes MongoDB.")
            return

        st.markdown("### Résultats détaillés des requêtes MongoDB")

        for code, objectif in st.session_state["requetes_objectifs"].items():
            df = st.session_state["resultats_mongo"].get(code)
            with st.expander(f"Requête {code} – {objectif}", expanded=False):
                st.markdown(f"**Objectif :** {objectif}")
                if df is None:
                    st.warning("Aucun résultat.")
                elif df.empty:
                    st.info("Résultat vide.")
                else:
                    st.dataframe(
                        df.style.set_properties(**{"text-align": "left"}),
                        width="stretch",
                    )


def render_partie_4_streamlit(tab) -> None:
    """
    Affiche l'onglet "Partie 4 : Tableau de bord et cartographie".

    Cet onglet est un espace réservé pour des visualisations plus avancées
    construites à partir des données SQL ou MongoDB (cartes, graphiques, etc.).
    """
    with tab:
        st.subheader("Partie 4 : Tableau de bord et cartographie")
        st.info(
            "Espace réservé pour de futures visualisations (cartes, graphiques, "
            "indicateurs clés) basées sur le jeu de données Paris 2055."
        )


def comparer_dataframes_souple(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
) -> tuple[str, str]:
    """
    Compare deux DataFrames de manière tolérante pour valider la migration.

    La comparaison regarde :
    - la présence ou non des résultats ;
    - le nombre de lignes et de colonnes ;
    - le contenu, avec une tolérance sur les types et les arrondis numériques.

    Retour
    ------
    (str, str)
        - icône de statut ("✅", "⚠️", "❌") ;
        - message d'explication.
    """
    if df1 is None or df2 is None:
        return "❌", "L'un des deux résultats est manquant."

    if df1.empty and df2.empty:
        return "✅", "Les deux résultats sont vides : situation cohérente."

    if df1.empty or df2.empty:
        return "❌", f"Disparité : SQL contient {len(df1)} lignes, Mongo {len(df2)}."

    if len(df1) != len(df2):
        diff = abs(len(df1) - len(df2))
        return "⚠️", (
            f"Nombre de lignes différent : {len(df1)} (SQL) vs {len(df2)} (Mongo). "
            f"Écart de {diff}."
        )

    if len(df1.columns) != len(df2.columns):
        return "⚠️", (
            f"Colonnes différentes : {list(df1.columns)} "
            f"vs {list(df2.columns)}."
        )

    try:
        col_sort_1 = df1.columns[0]
        col_sort_2 = df2.columns[0]

        df1_sorted = df1.sort_values(by=col_sort_1).reset_index(drop=True)
        df2_sorted = df2.sort_values(by=col_sort_2).reset_index(drop=True)

        df1_sorted.columns = [c.lower() for c in df1_sorted.columns]
        df2_sorted.columns = [c.lower() for c in df2_sorted.columns]

        pd.testing.assert_frame_equal(
            df1_sorted,
            df2_sorted,
            check_dtype=False,
            check_exact=False,
            rtol=1e-3,
        )
        return "✅", "Les deux résultats sont identiques (à la tolérance numérique près)."
    except AssertionError:
        return (
            "⚠️",
            "Dimensions identiques, mais certaines valeurs diffèrent "
            "(ordre, arrondis ou types).",
        )
    except Exception as exc:
        return "❌", f"Erreur technique lors de la comparaison : {str(exc)}"


def render_partie_5_comparaison(tab) -> None:
    """
    Affiche l'onglet "Partie 5 : Validation de la migration (SQL vs NoSQL)".

    Pour chaque requête A → N, l'onglet présente :
    - un statut de cohérence (OK, approximation, échec) ;
    - les résultats SQL et MongoDB côte à côte ;
    - un score global de validation.
    """
    with tab:
        st.subheader("Partie 5 : Validation de la Migration (SQL vs NoSQL)")
        st.markdown(
            "Cet onglet permet de vérifier si la transposition des requêtes SQL "
            "en MongoDB restitue bien les mêmes informations métier."
        )

        sql_ready = st.session_state.get("queries_sql_executed", False)
        mongo_ready = st.session_state.get("queries_mongo_executed", False)

        if not sql_ready or not mongo_ready:
            st.warning(
                "⚠️ Veuillez d'abord exécuter les requêtes de la **Partie 1 (SQL)** "
                "et de la **Partie 3 (MongoDB)** avant de lancer la comparaison."
            )
            return

        st.markdown("---")

        res_sql = st.session_state["resultats_sql"]
        res_mongo = st.session_state["resultats_mongo"]
        objectifs = st.session_state["requetes_objectifs"]

        total_ok = 0
        total_queries = len(objectifs)

        for code, objectif in objectifs.items():
            df_sql = res_sql.get(code)
            df_mongo = res_mongo.get(code)

            icon, message = comparer_dataframes_souple(df_sql, df_mongo)
            if icon == "✅":
                total_ok += 1

            with st.expander(f"{icon} Requête {code} : {objectif}"):
                st.caption(f"**Objectif :** {objectif}")

                if icon == "✅":
                    st.success(f"Résultat : {message}")
                elif icon == "⚠️":
                    st.warning(f"Résultat : {message}")
                else:
                    st.error(f"Résultat : {message}")

                col_a, col_b = st.columns(2)

                with col_a:
                    st.markdown("**1. Résultat SQL (source)**")
                    if df_sql is not None and not df_sql.empty:
                        st.dataframe(df_sql, width="stretch", height=200)
                        st.caption(
                            f"Lignes : {len(df_sql)} | "
                            f"Colonnes : {len(df_sql.columns)}"
                        )
                    else:
                        st.info("Résultat SQL vide ou non disponible.")

                with col_b:
                    st.markdown("**2. Résultat MongoDB (cible)**")
                    if df_mongo is not None and not df_mongo.empty:
                        st.dataframe(df_mongo, width="stretch", height=200)
                        st.caption(
                            f"Lignes : {len(df_mongo)} | "
                            f"Colonnes : {len(df_mongo.columns)}"
                        )
                    else:
                        st.info("Résultat MongoDB vide ou non disponible.")

        st.markdown("---")

        score = int((total_ok / total_queries) * 100)
        if score == 100:
            st.success(
                f"🏆 Migration validée à 100% "
                f"({total_ok}/{total_queries} requêtes strictement identiques)."
            )
        elif score > 80:
            st.success(
                f"✅ Migration validée à {score}% "
                f"({total_ok}/{total_queries} requêtes identiques)."
            )
        else:
            st.error(
                f"❌ Attention : seulement {score}% de correspondance "
                f"({total_ok}/{total_queries}). Il est conseillé d'analyser les écarts."
            )

# =====================================================================
# PARTIE 12 - ASSISTANT IA (GROQ / LLAMA 3.3)
# =====================================================================


def interroger_groq(question: str) -> tuple[Optional[Dict], Optional[str]]:
    """
    Appelle l'API Groq (modèle Llama 3.3) pour générer un pipeline MongoDB en JSON.

    L'IA reçoit le contexte de schéma (SCHEMA_CONTEXT) et produit un objet JSON
    contenant au minimum :
    - le nom de la collection cible ;
    - un pipeline d'agrégation MongoDB.

    Paramètres
    ----------
    question : str
        Question métier posée par l'utilisateur (en langage naturel).

    Retour
    ------
    (dict | None, str | None)
        - le JSON produit par l'IA (ou None si erreur) ;
        - un message d'erreur éventuel.
    """
    api_key = st.session_state.get("groq_api_key", "")

    if not api_key or "gsk_" not in api_key:
        return None, "Clé API Groq absente ou invalide."

    client = Groq(api_key=api_key)

    try:
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {
                    "role": "system",
                    "content": (
                        SCHEMA_CONTEXT
                        + "\n\nIMPORTANT : Analyse la question avec soin. "
                        "Si une moyenne ou un total existe déjà dans le modèle "
                        "(ex. 'stats_trafic' ou 'co2_moyen_ligne'), exploite-le "
                        "au lieu de recalculer depuis les documents bruts. "
                        "La réponse doit être un JSON strictement valide."
                    ),
                },
                {
                    "role": "user",
                    "content": f"La question est : {question}",
                },
            ],
            temperature=0,
            stream=False,
            response_format={"type": "json_object"},
        )

        response_content = completion.choices[0].message.content
        data = json.loads(response_content)
        return data, None

    except json.JSONDecodeError:
        return None, "La réponse retournée par l'IA n'est pas un JSON valide."
    except Exception as exc:
        return None, str(exc)


def analyser_resultats_avec_ia(
    question_user: str,
    df: pd.DataFrame,
    api_key: str,
) -> str:
    """
    Demande à l'IA une synthèse compréhensible du DataFrame obtenu par MongoDB.

    Le texte généré adapte son style :
    - si le résultat ne contient qu'une ligne, l'analyse est directe ;
    - s'il contient plusieurs lignes, l'accent est mis sur les tendances
      générales (valeurs extrêmes, ordre de grandeur, etc.).

    Paramètres
    ----------
    question_user : str
        Question initiale posée par l'utilisateur.
    df : pandas.DataFrame
        Résultat de la requête retournée par MongoDB.
    api_key : str
        Clé API Groq.

    Retour
    ------
    str
        Interprétation courte, en français, orientée métier.
    """
    if df.empty:
        return "Je n'ai trouvé aucun résultat correspondant à cette requête."

    nb_lignes = len(df)
    data_sample = df.head(10).to_string(index=False)

    stats_context = ""
    consigne_adaptative = ""

    if nb_lignes == 1:
        consigne_adaptative = (
            "Le tableau ne contient qu'une seule ligne. "
            "Ne produis pas de statistiques globales : explique simplement "
            "ce que cette ligne signifie par rapport à la question."
        )
    else:
        nums = df.select_dtypes(include=["number"])
        if not nums.empty:
            try:
                stats_desc = nums.describe().to_string()
                stats_context = f"\nSTATISTIQUES (pour toi) :\n{stats_desc}\n"
                consigne_adaptative = (
                    "Le tableau contient plusieurs lignes. "
                    "Mets en avant les valeurs marquantes (fortes/faibles), "
                    "la tendance globale et les ordres de grandeur."
                )
            except Exception:
                consigne_adaptative = (
                    "Résume les informations clés visibles dans le tableau."
                )
        else:
            consigne_adaptative = (
                "Les colonnes sont principalement textuelles. "
                "Mets en avant les éléments les plus représentatifs ou fréquents."
            )

    prompt = (
        f"QUESTION UTILISATEUR : '{question_user}'.\n\n"
        f"APERÇU DU RÉSULTAT (quelques lignes) :\n{data_sample}\n"
        f"{stats_context}\n"
        f"CONSIGNE : Tu es un analyste de données. {consigne_adaptative}\n"
        "Réponds en deux phrases maximum, en français, dans un style clair et professionnel."
    )

    client = Groq(api_key=api_key)
    try:
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {
                    "role": "system",
                    "content": "Tu es un assistant analyste de données concis et pédagogique.",
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0.3,
        )
        return completion.choices[0].message.content
    except Exception as exc:
        return f"Analyse IA non disponible : {str(exc)}"


def render_partie_6_ia(tab) -> None:
    """
    Affiche l'onglet "Partie 6 : Assistant IA".

    Fonctionnalités proposées :
    - poser une question métier en langage naturel ;
    - laisser l'IA proposer un pipeline d'agrégation MongoDB ;
    - exécuter ce pipeline sur la base Paris 2055 ;
    - obtenir une interprétation automatique des résultats.
    """
    QUESTION_BUTTONS = [
        "la moyenne des retards (en minutes) pour chaque ligne de transport.",
        "les 5 quartiers ayant la moyenne de niveau de bruit (en dB) la plus élevée, basée sur les capteurs de bruit aux arrêts.",
    ]

    if "ai_json_response" not in st.session_state:
        st.session_state["ai_json_response"] = None
    if "question_a_traiter" not in st.session_state:
        st.session_state["question_a_traiter"] = ""

    def set_question(q: str) -> None:
        """Met à jour dans la session la question choisie parmi les raccourcis."""
        st.session_state["question_a_traiter"] = q

    def set_question_from_input() -> None:
        """Récupère la question saisie dans la zone de texte et la stocke en session."""
        st.session_state["question_a_traiter"] = st.session_state.get(
            "ai_question_input",
            "",
        )

    with tab:
        st.subheader("Partie 6 : Assistant IA 🧠 (Llama 3.3)")
        st.markdown(
            "Modèle utilisé : **llama-3.3-70b-versatile** (Groq) pour générer "
            "des pipelines MongoDB en JSON strict."
        )

        st.text_area(
            "💬 Posez votre question sur les données Paris 2055 :",
            key="ai_question_input",
            height=70,
        )
        st.button(
            "✨ Générer & Exécuter le pipeline",
            type="primary",
            on_click=set_question_from_input,
        )

        results_container = st.container()

        st.markdown("---")
        st.caption("Questions rapides (exemples) :")
        cols = st.columns(len(QUESTION_BUTTONS))
        for i, question_text in enumerate(QUESTION_BUTTONS):
            cols[i].button(
                question_text,
                key=f"quick_q_{i}",
                on_click=set_question,
                args=(question_text,),
            )

        question_actuelle = st.session_state["question_a_traiter"].strip()

        if question_actuelle:
            with results_container:
                st.info(f"Question sélectionnée : {question_actuelle}")

                with st.spinner("Génération du pipeline MongoDB par l'IA..."):
                    result_ia, error = interroger_groq(question_actuelle)

                st.session_state["ai_json_response"] = result_ia

                if st.session_state.get("ai_json_response"):
                    with st.expander(
                        "🛠️ JSON technique généré (pipeline MongoDB)",
                        expanded=False,
                    ):
                        st.code(
                            json.dumps(
                                st.session_state["ai_json_response"],
                                indent=2,
                                ensure_ascii=False,
                            ),
                            language="json",
                        )

                if error:
                    st.error(f"Erreur lors de l'appel IA : {error}")
                    return

                collection_cible = result_ia.get("collection")
                pipeline = result_ia.get("pipeline")

                if pipeline is None or not isinstance(pipeline, list):
                    st.error(
                        "❌ L'IA n'a pas fourni de pipeline MongoDB valide "
                        "(liste de stages attendue)."
                    )
                    return

                if len(pipeline) == 0:
                    st.warning(
                        "🤖 La question semble en dehors du périmètre de la base "
                        "de données Paris 2055. Reformulez votre demande."
                    )
                    return

                with st.spinner(
                    f"Exécution du pipeline sur la collection '{collection_cible}'..."
                ):
                    try:
                        client = pymongo.MongoClient(MONGO_URI)
                        db = client[MONGO_DB_NAME]

                        if collection_cible not in db.list_collection_names():
                            st.error(
                                f"Erreur : la collection '{collection_cible}' est introuvable dans MongoDB."
                            )
                            client.close()
                            return

                        results = list(db[collection_cible].aggregate(pipeline))
                        client.close()

                        if results:
                            st.markdown(f"### 📊 Résultats ({len(results)} documents)")
                            df_res = pd.DataFrame(results)
                            if "_id" in df_res.columns:
                                df_res["_id"] = df_res["_id"].astype(str)

                            st.dataframe(df_res, width="stretch")

                            st.markdown("### 💡 Interprétation automatique")
                            with st.spinner("Analyse des résultats par l'IA..."):
                                analyse = analyser_resultats_avec_ia(
                                    question_actuelle,
                                    df_res,
                                    st.session_state["groq_api_key"],
                                )
                            st.info(analyse, icon="📈")
                        else:
                            st.warning(
                                "La requête est valide mais ne retourne aucun document (tableau vide)."
                            )

                    except Exception as exc:
                        st.error(f"Erreur MongoDB lors de l'exécution du pipeline : {exc}")

# =====================================================================
# PARTIE 13 - FONCTION PRINCIPALE STREAMLIT
# =====================================================================


def main() -> None:
    """
    Point d'entrée principal de l'application Streamlit.

    La fonction :
    - configure la page Streamlit (titre, layout) ;
    - initialise l'état de session ;
    - construit la barre latérale (statut systèmes, configuration, reset) ;
    - déclare les différents onglets fonctionnels.
    """
    global MONGO_URI

    st.set_page_config(
        page_title="Paris 2055 - Requêtes et Migration vers MongoDB",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    init_session_state()

    st.title("Paris 2055 - Requêtes et Migration vers MongoDB")

    with st.sidebar:
        st.header("📡 État du système")

        sqlite_exists = os.path.exists(DB_FILE)
        sqlite_icon = "✅" if sqlite_exists else "❌"
        sqlite_msg = "Ready" if sqlite_exists else "Missing"

        mongo_status = "Disconnected"
        mongo_icon = "❌"
        mongo_color = "red"

        current_uri = st.session_state.get("mongo_uri", MONGO_URI)

        try:
            client_check = pymongo.MongoClient(
                current_uri, serverSelectionTimeoutMS=500
            )
            client_check.admin.command("ping")
            if MONGO_DB_NAME in client_check.list_database_names():
                db_check = client_check[MONGO_DB_NAME]
                if db_check.lignes.count_documents({}) > 0:
                    mongo_status = "Ready"
                    mongo_icon = "✅"
                    mongo_color = "green"
                else:
                    mongo_status = "Empty"
                    mongo_icon = "⚠️"
                    mongo_color = "orange"
            else:
                mongo_status = "Empty"
                mongo_icon = "❌"
                mongo_color = "red"
            client_check.close()
        except Exception:
            mongo_status = "Offline"
            mongo_icon = "🚫"

        col1, col2 = st.columns(2)
        with col1:
            st.caption("SQLite")
            st.markdown(f"**{sqlite_icon} {sqlite_msg}**")
        with col2:
            st.caption("MongoDB")
            st.markdown(f":{mongo_color}[**{mongo_icon} {mongo_status}**]")

        st.markdown("---")

        st.header("🗂️ État des caches")
        if st.session_state.get("queries_sql_executed", False):
            st.success("Cache SQL : **Chargé**", icon="✅")
        else:
            st.info("Cache SQL : **Vide**", icon="⚪")

        if st.session_state.get("queries_mongo_executed", False):
            st.success("Cache Mongo : **Chargé**", icon="✅")
        else:
            st.info("Cache Mongo : **Vide**", icon="⚪")

        st.markdown("---")

        st.header("🔑 Configuration API & Base de données")

        new_key = st.text_input(
            "Groq API Key",
            value=st.session_state["groq_api_key"],
            type="password",
        )
        if new_key != st.session_state["groq_api_key"]:
            st.session_state["groq_api_key"] = new_key
            os.environ["GROQ_API_KEY"] = new_key
            try:
                env_path = find_dotenv() or ".env"
                set_key(env_path, "GROQ_API_KEY", new_key)
                st.success("Clé API Groq sauvegardée. ✅")
            except Exception:
                pass
            time.sleep(0.5)
            st.rerun()

        if "mongo_uri" not in st.session_state:
            st.session_state["mongo_uri"] = os.getenv(
                "MONGO_URI",
                "mongodb://127.0.0.1:27017/",
            )

        new_mongo = st.text_input(
            "MongoDB URI",
            value=st.session_state["mongo_uri"],
            placeholder="mongodb://...",
        )

        if new_mongo != st.session_state["mongo_uri"]:
            st.session_state["mongo_uri"] = new_mongo
            os.environ["MONGO_URI"] = new_mongo
            MONGO_URI = new_mongo

            try:
                env_path = find_dotenv() or ".env"
                set_key(env_path, "MONGO_URI", new_mongo)
                st.success("URI MongoDB sauvegardée. ✅")
            except Exception as exc:
                st.warning(f"Erreur lors de la mise à jour du fichier .env : {exc}")

            time.sleep(0.5)
            st.rerun()

        st.markdown("---")

        with st.expander("🧨 Danger Zone", expanded=False):
            st.error("Zone critique : actions irréversibles")

            st.write(
                "Cette action va :"
                "\n- Supprimer la base MongoDB 'Paris2055'."
                "\n- Effacer tous les fichiers CSV générés."
                "\n- Vider le cache de l'application."
            )

            confirm_delete = st.checkbox(
                "Je comprends les conséquences et je veux réinitialiser.",
                key="security_check",
                help="Cochez cette case pour déverrouiller le bouton de réinitialisation.",
            )

            if st.button(
                "🗑️ RÉINITIALISER L'APPLICATION",
                type="primary",
                use_container_width=True,
                disabled=not confirm_delete,
            ):
                with st.spinner("Nettoyage complet en cours..."):
                    try:
                        client = pymongo.MongoClient(MONGO_URI)
                        if MONGO_DB_NAME in client.list_database_names():
                            client.drop_database(MONGO_DB_NAME)
                        client.close()

                        for folder in [DOSSIER_CSV, DOSSIER_MONGO_CSV]:
                            if os.path.exists(folder):
                                for filename in os.listdir(folder):
                                    file_path = os.path.join(folder, filename)
                                    try:
                                        if os.path.isfile(file_path):
                                            os.unlink(file_path)
                                    except Exception as exc:
                                        console_log(
                                            f"Erreur lors de la suppression de {file_path} : {exc}"
                                        )

                        st.cache_data.clear()

                        keys_to_reset = [
                            "queries_mongo_executed",
                            "resultats_mongo",
                            "queries_sql_executed",
                            "resultats_sql",
                            "migration_logs",
                            "migration_running",
                            "migration_done_msg",
                            "ai_json_response",
                            "initialized",
                        ]
                        for key in keys_to_reset:
                            if key in st.session_state:
                                del st.session_state[key]

                        st.toast("Application réinitialisée. 💥", icon="✅")
                        time.sleep(2)
                        st.rerun()

                    except Exception as exc:
                        st.error(f"Erreur pendant le nettoyage : {exc}")

    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(
        [
            "Partie 1 : Requêtes SQL",
            "Partie 2 : Migration",
            "Partie 3 : Requêtes MongoDB",
            "Partie 4 : Dashboard",
            "Partie 5 : Comparaison",
            "Partie 6 : Assistant IA",
        ]
    )

    render_partie_1_sqlite(tab1)
    render_partie_2_migration(tab2)
    render_partie_3_mongo(tab3)
    render_partie_4_streamlit(tab4)
    render_partie_5_comparaison(tab5)
    render_partie_6_ia(tab6)


if __name__ == "__main__":
    print("le premier lancement de l'application peut prendre du temps")
    main()
