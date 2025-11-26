# ================ Lancez l'application avec : =========================
# streamlit run app.py
# ======================================================================

# ======================================================================
# Partie 0 - Imports et configuration globale
# ======================================================================

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
import re


# ======================================================================
# Partie 1 - Constantes de chemins et paramètres généraux
# ======================================================================

# Répertoires de travail locaux (fichiers SQLite, exports CSV, JSON, etc.)
DOSSIER_DATA = "data"
DOSSIER_SQLITE = "sqlite"

# Fichier principal de la base relationnelle source
DB_FILE = os.path.join(DOSSIER_DATA, DOSSIER_SQLITE, "db", "paris2055.sqlite")

# Dossiers d’export CSV (résultats des requêtes SQL et MongoDB)
DOSSIER_CSV = "./data/sqlite/resultats_requetes_sqlite/"
DOSSIER_JSON = os.path.join(DOSSIER_DATA, "mongodb", "collections")
DOSSIER_MONGO_CSV = os.path.join(
    DOSSIER_DATA,
    "mongodb",
    "resultats_requetes_mongodb",
)

# Paramètres de connexion à MongoDB (cible de la migration)
MONGO_URI = "mongodb://127.0.0.1:27017/"
MONGO_DB_NAME = "Paris2055"

# Chargement des variables d’environnement (dont la clé Groq)
load_dotenv()
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

# ======================================================================
# Partie 2 - Contexte de schéma pour l’IA (Génération de pipelines MongoDB)
# ======================================================================

SCHEMA_CONTEXT = """
Tu es un expert MongoDB et Python. Ton but est de traduire une question naturelle en pipeline d'agrégation MongoDB.

Voici le schéma EXACT de la base de données 'Paris2055' (Optimisée Document) :

1. Collection 'lignes' :
   - C'est la collection principale pour le trafic, les véhicules et le personnel.
   - Structure :
     {
       "id_ligne": int,
       "nom_ligne": str,
       "type": str (ex: "Bus", "Tramway", "Metro"),
       "frequentation_moyenne": float,
       "co2_moyen_ligne": float (Pré-calculé),
       
       # CHAMPS OPTIMISÉS (CACHES) - À utiliser en priorité :
       "stats_trafic": { "total_retard": float, "nb_trajets": int, "moyenne_precalc": float },
       "vehicules_cache": [ { "id_vehicule": int, "immatriculation": str, "type_vehicule": str (ex: "Electrique", "Diesel") } ],
       "chauffeurs_cache": [ { "id_chauffeur": int, "nom_chauffeur": str } ],

       # CHAMPS DÉTAILLÉS (Lourds) :
       "arrets": [ { "id_arret": int, "nom": str, "quartiers": [...] } ],
       "trafic": [ { "id_trafic": int, "retard_minutes": int, "incidents": [...] } ]
     }

2. Collection 'capteurs' :
   - Pour les mesures environnementales (Bruit, CO2, Temperature).
   - Structure :
     {
       "id_capteur": int,
       "type_capteur": str,
       "position": { "type": "Point", "coordinates": [long, lat] },
       "arret": { "id_arret": int, "nom": str, "nom_ligne": str }, # Info dénormalisée
       "mesures": [ { "valeur": float, "horodatage": date, "unite": str } ]
     }

3. Collection 'quartiers' :
   - Pour les analyses géographiques.
   - Structure :
     {
       "id_quartier": int,
       "nom": str,
       "geom": { "type": "Polygon", ... },
       "arrets": [ { "id_arret": int, "nom": str, "nom_ligne": str } ] # Liste des arrêts dans ce quartier
     }

RÈGLES DE GÉNÉRATION DU PIPELINE :

1. FORMAT DE SORTIE :
   Tu dois répondre UNIQUEMENT un objet JSON valide :
   {
     "collection": "nom_de_la_collection_cible",
     "pipeline": [ ... liste des étapes d'agrégation ... ]
   }

2. RÈGLES MÉTIER & OPTIMISATION :
   - Si la question porte sur les VÉHICULES (ex: "combien de bus électriques"), utilise `vehicules_cache` dans la collection `lignes`. N'invente pas de collection "vehicules".
   - Si la question porte sur les CHAUFFEURS, utilise `chauffeurs_cache` dans `lignes`.
   - Si la question porte sur le RETARD GLOBAL d'une ligne, privilégie `stats_trafic` si disponible, sinon déroule `trafic`.
   - Pour filtrer une liste (ex: garder les véhicules électriques), utilise `$unwind` sur le cache correspondant puis `$match`.

3. RÈGLE DE PROJECTION (OBLIGATOIRE) :
   - Termine TOUJOURS par un `"$project"`.
   - Supprime `_id` (`"_id": 0`).
   - Ne renvoie QUE les champs utiles à la réponse (ex: `nom_ligne`, `valeur_calculee`).
   - NE RENVOIE JAMAIS les listes complètes (`trafic`, `mesures`, `arrets`) sauf demande explicite.

4. EXEMPLES :
   - "Moyenne retard par chauffeur ?" -> Collection: `lignes` -> $unwind `chauffeurs_cache` -> $project nom & stats_trafic.moyenne_precalc.
   - "Quartiers les plus bruyants ?" -> Collection: `quartiers` -> $lookup sur capteurs (via arrets) -> moyenne.

5. CAS D'ERREUR :
   Si la question est hors sujet (ex: "Recette de cuisine"), renvoie :
   { "collection": "lignes", "pipeline": [] }
"""


# ======================================================================
# Partie 3 - Utilitaires génériques (indépendants de SQLite / Mongo / UI)
# ======================================================================

def log_progress(
    current: int,
    total: int,
    prefix: str,
    log_fn: Callable[[str, bool], None],
    step_percent: Optional[int] = None,
) -> None:
    """
    Affiche une progression textuelle d'un traitement long.

    La fonction calcule le pourcentage d'avancement et délègue l'affichage
    à une fonction de log externe (console, Streamlit, etc.). Elle peut
    limiter la fréquence d'affichage pour éviter de saturer les sorties.

    Paramètres
    ----------
    current : int
        Nombre d'éléments déjà traités.
    total : int
        Nombre total d'éléments à traiter.
    prefix : str
        Libellé fonctionnel du traitement (ex. 'Migration lignes').
    log_fn : Callable[[str, bool], None]
        Fonction de log recevant le message et un booléen indiquant
        si la dernière ligne doit être remplacée.
    step_percent : int | None
        Pas d'affichage en pourcentage (ex. 10 pour afficher tous les 10 %).
        Si None, chaque appel est potentiellement loggé.

    Remarque
    --------
    Si `total` vaut 0, la fonction ne produit aucun log (division évitée).
    """
    if total == 0:
        return

    if step_percent:
        step = max(int(total * (step_percent / 100)), 1)
        # Ne pas logguer chaque élément pour éviter un flot de messages.
        if current % step != 0 and current != total:
            return

    pct = (current / total) * 100
    message = f"    >> {prefix} : {current:,} / {total:,} ({pct:.1f}%)"
    log_fn(message, replace_last=True)


def parse_geojson_geometry(geojson_str: Optional[str]) -> Optional[Dict]:
    """
    Extrait la géométrie (type + coordonnées) à partir d'une chaîne GeoJSON.

    La fonction accepte aussi bien :
    - un objet GeoJSON de type Geometry (Polygon, Point, etc.) ;
    - un objet GeoJSON de type Feature contenant un champ 'geometry'.

    Paramètres
    ----------
    geojson_str : str | None
        Chaîne JSON représentant un objet GeoJSON ou une Feature.

    Retour
    ------
    dict | None
        Dictionnaire minimal de la forme :
        {
            "type": <type_geojson>,
            "coordinates": <structure_de_coordonnées>
        }
        ou None si la chaîne est vide, invalide ou ne contient pas de géométrie exploitable.
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
        # Cas où la chaîne représente directement une géométrie.
        if "type" in obj and "coordinates" in obj:
            return {"type": obj["type"], "coordinates": obj["coordinates"]}

        # Cas Feature GeoJSON : la géométrie est dans obj["geometry"].
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
    Devine une unité de mesure raisonnable à partir du type de capteur.

    Cette fonction sert de repli lorsque la colonne 'unite' n'est pas
    renseignée dans les mesures. La logique est volontairement simple et
    basée sur quelques mots-clés.

    Paramètres
    ----------
    type_capteur : str | None
        Type fonctionnel du capteur (ex. 'Capteur Bruit', 'Capteur CO2').

    Retour
    ------
    str | None
        Unité suggérée :
        - 'dB' pour les capteurs de bruit,
        - '°C' pour la température,
        - 'ppm' pour le CO2,
        - None si aucun motif n'est reconnu.
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


def to_datetime(value) -> Optional[object]:
    """
    Convertit une valeur vers un objet datetime Python si possible.

    La conversion s'appuie sur `pandas.to_datetime` en mode tolérant
    (erreurs silencieuses). Les valeurs non convertibles sont renvoyées
    sous forme de None.

    Paramètres
    ----------
    value :
        Valeur initiale (chaîne, timestamp, etc.) ou NaN.

    Retour
    ------
    datetime | None
        Objet datetime Python si la conversion réussit, None sinon.
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

# =====================================================================
# UTILITAIRES MONGO / ETL (COUCHE DATA, HORS UI)
# =====================================================================

def aggregate_to_df(collection, pipeline: List[Dict]) -> pd.DataFrame:
    """
    Exécute un pipeline d'agrégation MongoDB et renvoie le résultat
    sous forme de DataFrame.

    Paramètres
    ----------
    collection :
        Instance de collection MongoDB (ex. db.lignes).
    pipeline : list[dict]
        Pipeline d'agrégation à appliquer sur la collection.

    Retour
    ------
    pandas.DataFrame
        DataFrame contenant les documents retournés par l'agrégation.
        DataFrame vide si aucun document n'est renvoyé.
    """
    documents = list(collection.aggregate(pipeline))
    if not documents:
        return pd.DataFrame()
    return pd.DataFrame(documents)


def dataframe_to_dict_progressive(
    df: pd.DataFrame,
    label: str,
    log_fn: Callable[[str, bool], None],
    batch_size: int = 1000,
) -> List[Dict]:
    """
    Convertit un DataFrame en liste de dictionnaires avec suivi de
    progression par paquets.

    Utile pour préparer des documents à insérer en base ou à sérialiser,
    tout en limitant la consommation mémoire et le nombre de messages de log.

    Paramètres
    ----------
    df : pandas.DataFrame
        Données tabulaires à convertir.
    label : str
        Libellé utilisé dans les messages de progression.
    log_fn : Callable[[str, bool], None]
        Fonction de log prenant un message et un indicateur de remplacement.
    batch_size : int
        Nombre de lignes converties à chaque itération.

    Retour
    ------
    list[dict]
        Liste de dictionnaires (un par ligne du DataFrame).
    """
    total = len(df)
    documents: List[Dict] = []

    log_progress(0, total, label, log_fn)

    # Parcours par tranches pour éviter de charger l'ensemble d'un coup
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
    Sérialise une collection métier dans un fichier JSON sur disque.

    Paramètres
    ----------
    nom_collection : str
        Nom logique de la collection (utilisé dans le nom du fichier).
    data : list[dict]
        Documents à sérialiser.
    log_fn : Callable[[str, bool], None]
        Fonction de log pour tracer l'opération.

    Retour
    ------
    str
        Nom du fichier JSON créé (sans le chemin complet).
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
            # default=str : tolère les types non sérialisables nativement (datetime, Timestamp, etc.).
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
    Insère une liste de documents dans une collection MongoDB par
    paquets, en affichant l'avancement.

    Paramètres
    ----------
    collection :
        Collection MongoDB cible (ex. db.lignes).
    docs : list[dict]
        Documents à insérer.
    label : str
        Libellé fonctionnel de la collection (affiché dans les logs).
    batch_size : int
        Taille des paquets d'insertion.
    log_fn : Callable[[str, bool], None]
        Fonction de log (par défaut la fonction print).

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

    # Insertion par paquets pour éviter les requêtes trop volumineuses
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
# ETL COMPLET SQLITE -> DOCUMENTS -> MONGODB
# =====================================================================

def load_tables(
    conn: sqlite3.Connection,
    log_fn: Callable[[str, bool], None],
) -> Dict[str, pd.DataFrame]:
    """
    Charge les tables nécessaires depuis la base SQLite dans un
    dictionnaire de DataFrame.

    Paramètres
    ----------
    conn : sqlite3.Connection
        Connexion SQLite déjà ouverte sur le fichier de base.
    log_fn : Callable[[str, bool], None]
        Fonction de log utilisée pour tracer les étapes.

    Retour
    ------
    dict[str, pandas.DataFrame]
        Dictionnaire associant le nom de table à son DataFrame.
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
    Construit les documents de la collection 'lignes' à partir des
    tables relationnelles.

    Le document de sortie par ligne contient :
    - les caractéristiques de la ligne ;
    - des sous-documents d'arrêts, horaires, capteurs et trafic ;
    - des caches optimisés pour certains calculs (chauffeurs, véhicules,
      statistiques de trafic, CO2 moyen).
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

    # 1. Calcul du CO2 moyen par ligne (à partir des mesures CO2 aux arrêts)
    co2_by_ligne: Dict[int, float] = {}
    if not df_cap.empty and not df_mes.empty:
        df_full = df_cap.merge(df_mes, on="id_capteur")
        df_co2 = df_full[df_full["type_capteur"] == "CO2"]
        df_co2_ligne = df_co2.merge(df_a[["id_arret", "id_ligne"]], on="id_arret")
        if not df_co2_ligne.empty:
            co2_by_ligne = (
                df_co2_ligne.groupby("id_ligne")["valeur"].mean().to_dict()
            )

    # 2. Préparation du cache chauffeurs par ligne (pour la requête K optimisée)
    log_fn("⚡ [OPTIM] Construction du cache Chauffeurs...", replace_last=False)
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

    # 3. Préparation du cache véhicules par ligne (pour les requêtes D et L)
    log_fn("⚡ [OPTIM] Construction du cache Véhicules...", replace_last=False)
    vehicules_cache_by_ligne: Dict[int, List[Dict]] = {}
    if not df_v.empty:
        # On suppose que df_v contient la colonne id_ligne (ligne officielle du véhicule)
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

    # 4. Pré-calcul des statistiques de trafic par ligne et détail des incidents
    log_fn("⚡ [OPTIM] Pré-calcul des stats Trafic...", replace_last=False)
    stats_trafic_by_ligne: Dict[int, Dict] = {}
    trafic_by_ligne: Dict[int, List[Dict]] = {}

    incidents_by_trafic: Dict[int, List[Dict]] = {}
    if not df_i.empty:
        for id_trafic, group in df_i.groupby("id_trafic"):
            incidents_by_trafic[id_trafic] = group[
                ["id_incident", "description", "gravite"]
            ].to_dict("records")

    if not df_t.empty:
        # Agrégation vectorisée via Pandas pour obtenir les stats par ligne
        stats_group = df_t.groupby("id_ligne")["retard_minutes"].agg(
            ["sum", "count", "mean"]
        )

        for id_ligne, row_stat in stats_group.iterrows():
            stats_trafic_by_ligne[int(id_ligne)] = {
                "total_retard": float(row_stat["sum"]),
                "nb_trajets": int(row_stat["count"]),
                "moyenne_precalc": float(row_stat["mean"]),
            }

        # Construction des sous-documents de trafic (liste par ligne)
        for _, row in df_t.iterrows():
            if pd.isna(row["id_ligne"]):
                continue
            tdoc: Dict[str, object] = {"id_trafic": int(row["id_trafic"])}
            if pd.notnull(row.get("retard_minutes")):
                tdoc["retard_minutes"] = int(row["retard_minutes"])
            if row["id_trafic"] in incidents_by_trafic:
                tdoc["incidents"] = incidents_by_trafic[row["id_trafic"]]
            trafic_by_ligne.setdefault(int(row["id_ligne"]), []).append(tdoc)

    # 5. Préparations pour arrêts, quartiers, capteurs et horaires
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

    capteurs_ids_by_arret: Dict[int, List[int]] = {}
    if not df_cap.empty:
        for id_arret, group in df_cap.groupby("id_arret"):
            capteurs_ids_by_arret[id_arret] = [
                int(v) for v in group["id_capteur"].dropna().unique().tolist()
            ]

    horaires_by_arret: Dict[int, List[Dict]] = {}
    if not df_h.empty:
        # On rattache les véhicules aux horaires pour enrichir le sous-document
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

    # Consolidation des arrêts par ligne (incluant quartiers, horaires, capteurs)
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

        # Coordonnées géographiques si disponibles
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

    # 6. Assemblage final des documents 'lignes'
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

        # Champs optimisés / dérivés
        if id_l in co2_by_ligne:
            doc["co2_moyen_ligne"] = co2_by_ligne[id_l]
        if id_l in chauffeurs_cache_by_ligne:
            doc["chauffeurs_cache"] = chauffeurs_cache_by_ligne[id_l]
        if id_l in vehicules_cache_by_ligne:
            doc["vehicules_cache"] = vehicules_cache_by_ligne[id_l]
        if id_l in stats_trafic_by_ligne:
            doc["stats_trafic"] = stats_trafic_by_ligne[id_l]

        # Sous-documents détaillés
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
    Construit les documents de la collection 'quartiers' à partir des
    tables SQLite (quartiers, arrêts, lignes).

    Chaque quartier contient un polygone (geom) éventuel et la liste
    des arrêts qui s'y trouvent (avec la ligne associée).
    """
    df_q = tables["Quartier"]
    df_aq = tables["ArretQuartier"]
    df_a = tables["Arret"]
    df_l = tables["Ligne"]

    # 1. Regrouper les arrêts par quartier
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

    # 2. Construction finale des documents quartier
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
    Construit les documents de la collection 'capteurs' à partir des
    tables SQLite (capteurs, mesures, arrêts, lignes).

    Chaque document capteur contient :
    - ses métadonnées (type, position) ;
    - l'arrêt lié (avec ligne et nom) ;
    - la liste de ses mesures (valeur, horodatage, unité).
    """
    df_capteur = tables["Capteur"]
    df_mesure = tables["Mesure"]
    df_arret = tables["Arret"]
    df_ligne = tables["Ligne"]

    # 1. Préparer les informations d'arrêt associées à chaque capteur
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

    # 2. Mémoriser le type de chaque capteur (utile pour inférer l'unité)
    type_capteur_by_id: Dict[int, Optional[str]] = {}
    for _, row in df_capteur.iterrows():
        if pd.isna(row["id_capteur"]):
            continue

        capteur_id = int(row["id_capteur"])
        type_capteur_by_id[capteur_id] = row.get("type_capteur")

    # 3. Regrouper les mesures par capteur avec homogénéisation de l'unité
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

    # 4. Construction finale des documents capteur
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


def creer_index_mongodb(
    db: pymongo.database.Database,
    log_fn: Callable[[str, bool], None],
) -> None:
    """
    Crée les principaux index MongoDB nécessaires aux requêtes
    de l'application sur les collections 'lignes', 'capteurs' et
    'quartiers'.

    Paramètres
    ----------
    db : pymongo.database.Database
        Base MongoDB cible (ex. client[MONGO_DB_NAME]).
    log_fn : Callable[[str, bool], None]
        Fonction de log pour tracer la création des index.

    Retour
    ------
    None
    """
    log_fn("[Index] Création des index MongoDB...", replace_last=False)
    try:
        # Index sur les champs de recherche des lignes
        db.lignes.create_index("id_ligne")
        db.lignes.create_index("nom_ligne")
        db.lignes.create_index("type")

        # Index sur les capteurs et leur rattachement à un arrêt/ligne
        db.capteurs.create_index("id_capteur")
        db.capteurs.create_index("type_capteur")
        db.capteurs.create_index("arret.id_ligne")
        db.capteurs.create_index("arret.id_arret")

        # Index géospatiaux pour les quartiers et les capteurs
        db.quartiers.create_index([("geom", "2dsphere")])
        db.capteurs.create_index([("position", "2dsphere")])

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
    Effectue la migration complète des données depuis la base SQLite
    (modèle relationnel) vers la base MongoDB (modèle document).

    Étapes principales :
    1. Nettoyage des collections existantes dans MongoDB.
    2. Chargement des tables SQLite en DataFrames.
    3. Construction des documents pour :
       - lignes
       - quartiers
       - capteurs
    4. Sauvegarde intermédiaire en JSON.
    5. Insertion dans MongoDB par paquets.
    6. Création des index.

    Paramètres
    ----------
    log_fn_raw : Callable[[str, bool], None]
        Fonction de log fournie par l'IHM. Peut ou non supporter
        l'argument `replace_last` (géré par un wrapper interne).

    Retour
    ------
    None
    """

    def secure_log(message: str, replace_last: bool = False) -> None:
        """
        Wrapper de la fonction de log fournie par l'appelant.

        Si la fonction passée ne gère pas l'argument `replace_last`,
        on bascule sur un simple print pour éviter les plantages.
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
        Exécute le pipeline complet de construction et d'insertion
        pour une collection métier donnée.

        Paramètres
        ----------
        label : str
            Nom de la collection MongoDB et du fichier JSON (ex. 'lignes').
        build_func : Callable
            Fonction de construction des documents à partir des tables
            SQLite (ex. build_lignes_docs).
        tables : dict[str, pandas.DataFrame]
            Tables SQLite préchargées (voir load_tables).
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

        # Libération explicite de la mémoire associée à cette collection
        del documents

    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]

    secure_log("🚀 DÉBUT DE LA MIGRATION (ETL)", replace_last=False)
    secure_log(
        "   Mode : SQLite -> Modèle Document -> MongoDB",
        replace_last=False,
    )

    try:
        # 1. Suppression des anciennes collections (hygiène avant migration)
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
                    # Un échec de drop ne doit pas bloquer toute la migration
                    continue

        secure_log(
            f"   🗑️  {dropped_count} anciennes collections supprimées.",
            replace_last=False,
        )

        # 2. Ouverture de la base SQLite et chargement des tables
        if not os.path.exists(DB_FILE):
            raise FileNotFoundError(f"DB introuvable: {DB_FILE}")

        sqlite_conn = sqlite3.connect(DB_FILE)
        try:
            tables = load_tables(sqlite_conn, secure_log)
        finally:
            sqlite_conn.close()

        # 3. Traitement des collections métiers
        process_step("lignes", build_lignes_docs, tables)
        process_step("quartiers", build_quartiers_docs, tables)
        process_step("capteurs", build_capteurs_docs, tables)

        # 4. Création des index
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
# SQL METIER : définition des objectifs et exécution des requêtes SQLite
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


@st.cache_data(show_spinner=False)
def executer_toutes_les_requetes() -> Dict[str, pd.DataFrame]:
    """
    Exécute toutes les requêtes SQL métier (A → N) sur la base SQLite.
    UTILISE LE CACHE STREAMLIT : Ne recalcule pas si déjà fait.
    """
    if not os.path.exists(DB_FILE):
        return {}

    resultats: Dict[str, pd.DataFrame] = {}

    # Dictionnaire des requêtes (identique à avant)
    sql_queries: Dict[str, str] = {
        "A": "SELECT L.nom_ligne, AVG(T.retard_minutes) AS moyenne_retard_minutes FROM Trafic AS T JOIN Ligne AS L ON T.id_ligne = L.id_ligne GROUP BY L.nom_ligne ORDER BY moyenne_retard_minutes DESC;",
        "B": "SELECT L.id_ligne, AVG(T.total_passagers_jour) AS moyenne_passagers_jour FROM ( SELECT A.id_ligne, DATE(H.heure_prevue) AS jour, SUM(H.passagers_estimes) AS total_passagers_jour FROM Horaire AS H JOIN Arret AS A ON H.id_arret = A.id_arret GROUP BY A.id_ligne, jour ) AS T JOIN Ligne AS L ON T.id_ligne = L.id_ligne GROUP BY L.id_ligne ORDER BY moyenne_passagers_jour DESC;",
        "C": "SELECT L.nom_ligne, COUNT(DISTINCT I.id_trafic) AS nb_trafic_avec_incident, COUNT(DISTINCT T.id_trafic) AS nb_total_trafic, CASE WHEN COUNT(DISTINCT T.id_trafic) = 0 THEN 0 ELSE (CAST(COUNT(DISTINCT I.id_trafic) AS REAL) / COUNT(DISTINCT T.id_trafic)) * 100 END AS taux_incident_pourcent FROM Ligne AS L LEFT JOIN Trafic AS T ON L.id_ligne = T.id_ligne LEFT JOIN Incident AS I ON T.id_trafic = I.id_trafic GROUP BY L.nom_ligne ORDER BY taux_incident_pourcent DESC;",
        "D": "SELECT V.id_vehicule, V.immatriculation, AVG(M.valeur) AS moyenne_co2 FROM Vehicule AS V JOIN Ligne AS L ON V.id_ligne = L.id_ligne JOIN Arret AS A ON L.id_ligne = A.id_ligne JOIN Capteur AS C ON A.id_arret = C.id_arret JOIN Mesure AS M ON C.id_capteur = M.id_capteur WHERE C.type_capteur = 'CO2' GROUP BY V.id_vehicule, V.immatriculation ORDER BY moyenne_co2 DESC;",
        "E": "SELECT Q.nom, AVG(M.valeur) AS moyenne_bruit_db FROM Quartier AS Q JOIN ArretQuartier AS AQ ON Q.id_quartier = AQ.id_quartier JOIN Arret AS A ON AQ.id_arret = A.id_arret JOIN Capteur AS C ON A.id_arret = C.id_arret JOIN Mesure AS M ON C.id_capteur = M.id_capteur WHERE C.type_capteur = 'Bruit' GROUP BY Q.nom ORDER BY moyenne_bruit_db DESC LIMIT 5;",
        "F": "SELECT DISTINCT L.nom_ligne FROM Ligne AS L JOIN Trafic AS T ON L.id_ligne = T.id_ligne WHERE T.retard_minutes > 10 EXCEPT SELECT DISTINCT L.nom_ligne FROM Ligne AS L JOIN Trafic AS T ON L.id_ligne = T.id_ligne JOIN Incident AS I ON T.id_trafic = I.id_trafic;",
        "G": "SELECT COUNT(*) AS total_trajets, SUM(CASE WHEN retard_minutes = 0 THEN 1 ELSE 0 END) AS trajets_sans_retard, (CAST(SUM(CASE WHEN retard_minutes = 0 THEN 1 ELSE 0 END) AS REAL) / COUNT(*)) * 100 AS taux_ponctualite_global_pourcent FROM Trafic;",
        "H": "SELECT Q.nom, COUNT(AQ.id_arret) AS nombre_arrets FROM Quartier AS Q LEFT JOIN ArretQuartier AS AQ ON Q.id_quartier = AQ.id_quartier GROUP BY Q.nom ORDER BY nombre_arrets DESC;",
        "I": "WITH AvgRetard AS ( SELECT id_ligne, AVG(retard_minutes) AS moyenne_retard FROM Trafic GROUP BY id_ligne ), AvgCO2 AS ( SELECT A.id_ligne, AVG(M.valeur) AS moyenne_co2 FROM Mesure AS M JOIN Capteur AS C ON M.id_capteur = C.id_capteur JOIN Arret AS A ON C.id_arret = A.id_arret WHERE C.type_capteur = 'CO2' GROUP BY A.id_ligne ) SELECT L.nom_ligne, COALESCE(R.moyenne_retard, 0) AS moyenne_retard, COALESCE(C.moyenne_co2, 0) AS moyenne_co2 FROM Ligne AS L LEFT JOIN AvgRetard AS R ON L.id_ligne = R.id_ligne LEFT JOIN AvgCO2 AS C ON L.id_ligne = C.id_ligne ORDER BY L.nom_ligne;",
        "J": "SELECT L.nom_ligne, AVG(M.valeur) AS moyenne_temperature FROM Ligne AS L JOIN Arret AS A ON L.id_ligne = A.id_ligne JOIN Capteur AS C ON A.id_arret = C.id_arret JOIN Mesure AS M ON C.id_capteur = M.id_capteur WHERE C.type_capteur = 'Temperature' GROUP BY L.nom_ligne ORDER BY moyenne_temperature DESC;",
        "K": "SELECT C.nom, AVG(T.retard_minutes) AS moyenne_retard_minutes FROM Chauffeur AS C JOIN Vehicule AS V ON C.id_chauffeur = V.id_chauffeur JOIN Trafic AS T ON V.id_ligne = T.id_ligne GROUP BY C.nom ORDER BY moyenne_retard_minutes DESC;",
        "L": "SELECT L.nom_ligne, COUNT(V.id_vehicule) AS total_vehicules, SUM(CASE WHEN V.type_vehicule = 'Electrique' THEN 1 ELSE 0 END) AS nb_electriques, (CAST(SUM(CASE WHEN V.type_vehicule = 'Electrique' THEN 1 ELSE 0 END) AS REAL) / COUNT(V.id_vehicule)) * 100 AS pourcentage_electrique FROM Ligne AS L JOIN Vehicule AS V ON L.id_ligne = V.id_ligne WHERE L.type = 'Bus' GROUP BY L.nom_ligne ORDER BY pourcentage_electrique DESC;",
        "M": "SELECT C.id_capteur, C.latitude, C.longitude, AVG(M.valeur) AS moyenne_co2, CASE WHEN AVG(M.valeur) > 800 THEN 'Élevé' WHEN AVG(M.valeur) > 450 THEN 'Moyen' ELSE 'Faible' END AS niveau_pollution FROM Capteur AS C JOIN Mesure AS M ON C.id_capteur = M.id_capteur WHERE C.type_capteur = 'CO2' GROUP BY C.id_capteur, C.latitude, C.longitude ORDER BY moyenne_co2 DESC;",
        "N": "SELECT nom_ligne, type, frequentation_moyenne, CASE WHEN frequentation_moyenne > 2000 THEN 'Haute Fréquentation' WHEN frequentation_moyenne > 1000 THEN 'Moyenne Fréquentation' ELSE 'Basse Fréquentation' END AS categorie_frequentation FROM Ligne ORDER BY frequentation_moyenne DESC;",
    }

    with sqlite3.connect(DB_FILE) as conn:
        for code, query in sql_queries.items():
            try:
                # On ne fait que lire, pas d'écriture CSV ici
                df_result = pd.read_sql_query(query, conn)
                resultats[code] = df_result
            except Exception as exc:
                resultats[code] = pd.DataFrame([{"erreur": str(exc)}])

    return resultats

def forcer_ecriture_csv_sql(resultats: Dict[str, pd.DataFrame]) -> None:
    """
    Fonction NON CACHÉE : Écrit les résultats sur le disque.
    Appelée explicitement après le calcul.
    """
    os.makedirs(DOSSIER_CSV, exist_ok=True)
    print("💾 Sauvegarde CSV SQL sur disque...")
    for code, df in resultats.items():
        nom_fichier = f"resultat_req_{code.lower()}.csv"
        full_path = os.path.join(DOSSIER_CSV, nom_fichier)
        try:
            df.to_csv(full_path, index=False, encoding="utf-8-sig")
        except Exception as e:
            print(f"Erreur écriture {nom_fichier}: {e}")
    print("✅ CSV SQL sauvegardés.")

# =====================================================================
# MONGO METIER : requêtes d'analyse sur le modèle document
# =====================================================================

def query_A_mongo(db) -> pd.DataFrame:
    """
    Requête A (MongoDB) :
    Moyenne des retards par ligne (équivalent fonctionnel de la requête SQL A).

    Retourne un DataFrame avec :
    - nom_ligne
    - moyenne_retard_minutes
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
    return df[["nom_ligne", "moyenne_retard_minutes"]] if not df.empty else df


def query_B_mongo(db) -> pd.DataFrame:
    """
    Requête B (MongoDB) :
    Nombre moyen de passagers transportés par jour pour chaque ligne.

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
            },
        },
        {
            "$group": {
                "_id": {"id_ligne": "$id_ligne", "jour": "$jour"},
                "total_passagers_jour": {"$sum": "$passagers_estimes"},
            },
        },
        {
            "$group": {
                "_id": "$_id.id_ligne",
                "moyenne_passagers_jour": {"$avg": "$total_passagers_jour"},
            },
        },
        {
            "$project": {
                "_id": 0,
                "id_ligne": "$_id",
                "moyenne_passagers_jour": 1,
            },
        },
        {"$sort": {"moyenne_passagers_jour": -1}},
    ]

    df = aggregate_to_df(db.lignes, pipeline)
    return df[["id_ligne", "moyenne_passagers_jour"]] if not df.empty else df


def query_C_mongo(db) -> pd.DataFrame:
    """
    Requête C (MongoDB) :
    Taux d'incidents par ligne en pourcentage.

    Retourne un DataFrame avec :
    - nom_ligne
    - nb_trafic_avec_incident
    - nb_total_trafic
    - taux_incident_pourcent
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
    cols = [
        "nom_ligne",
        "nb_trafic_avec_incident",
        "nb_total_trafic",
        "taux_incident_pourcent",
    ]
    return df[cols] if not df.empty else df


def query_D_mongo(db) -> pd.DataFrame:
    """
    Requête D (MongoDB) :
    Moyenne de CO2 associée aux véhicules, en exploitant le modèle document.

    Stratégie
    ---------
    - Utilisation de 'co2_moyen_ligne' (pré-calculée par ligne).
    - Utilisation de 'vehicules_cache' pour associer chaque véhicule
      à la moyenne CO2 de sa ligne.
    - Évite un parcours coûteux des arrêts, capteurs et mesures.

    Retourne un DataFrame avec :
    - id_vehicule
    - immatriculation
    - moyenne_co2
    """
    pipeline = [
        {
            "$match": {
                "co2_moyen_ligne": {"$exists": True, "$ne": None},
                "vehicules_cache": {"$exists": True, "$ne": []},
            },
        },
        {
            "$project": {
                "vehicules_cache": 1,
                "co2_moyen_ligne": 1,
            },
        },
        {"$unwind": "$vehicules_cache"},
        {
            "$project": {
                "_id": 0,
                "id_vehicule": "$vehicules_cache.id_vehicule",
                "immatriculation": "$vehicules_cache.immatriculation",
                "moyenne_co2": "$co2_moyen_ligne",
            },
        },
        {"$sort": {"moyenne_co2": -1}},
    ]

    df = aggregate_to_df(db.lignes, pipeline)
    if df.empty:
        return pd.DataFrame(
            columns=["id_vehicule", "immatriculation", "moyenne_co2"],
        )
    return df[["id_vehicule", "immatriculation", "moyenne_co2"]]


def query_E_mongo(db) -> pd.DataFrame:
    """
    Requête E (MongoDB) :
    Quartiers les plus bruyants selon les capteurs de type 'Bruit'.

    Retourne un DataFrame avec :
    - nom (quartier)
    - moyenne_bruit_db
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
    Requête F (MongoDB) :
    Lignes ayant connu des retards > 10 minutes mais aucun incident.

    Retourne un DataFrame avec :
    - nom_ligne
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
                                                    ],
                                                },
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
    Requête G (MongoDB) :
    Taux global de ponctualité (trajets sans retard / total des trajets).

    Retourne un DataFrame avec :
    - total_trajets
    - trajets_sans_retard
    - taux_ponctualite_global_pourcent
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
    cols = [
        "total_trajets",
        "trajets_sans_retard",
        "taux_ponctualite_global_pourcent",
    ]
    return df[cols] if not df.empty else df


def query_H_mongo(db) -> pd.DataFrame:
    """
    Requête H (MongoDB) :
    Nombre d'arrêts par quartier.

    Retourne un DataFrame avec :
    - nom (quartier)
    - nombre_arrets
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
    Requête I (MongoDB) :
    Corrélation entre moyenne des retards et moyenne de CO2 par ligne.

    Retourne un DataFrame avec :
    - nom_ligne
    - moyenne_retard
    - moyenne_co2
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
                                                    ],
                                                },
                                                "as": "m",
                                                "in": "$$m.valeur",
                                            },
                                        },
                                    ],
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
    Requête J (MongoDB) :
    Température moyenne mesurée par ligne.

    Retourne un DataFrame avec :
    - nom_ligne
    - moyenne_temperature
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
    Requête K (MongoDB) :
    Moyenne des retards par chauffeur.

    Stratégie
    ---------
    - Exploite 'stats_trafic' (total_retard, nb_trajets) pré-calculés par ligne.
    - Exploite 'chauffeurs_cache' pour associer chaque chauffeur aux lignes.
    - Agrège les statistiques sur l'ensemble des lignes d'un même chauffeur.

    Retourne un DataFrame avec :
    - nom (chauffeur)
    - moyenne_retard_minutes
    """
    pipeline = [
        {
            "$match": {
                "stats_trafic": {"$exists": True},
                "chauffeurs_cache": {"$exists": True, "$ne": []},
            },
        },
        {
            "$project": {
                "chauffeurs_cache": 1,
                "total_retard": "$stats_trafic.total_retard",
                "nb_trajets": "$stats_trafic.nb_trajets",
            },
        },
        {"$unwind": "$chauffeurs_cache"},
        {
            "$group": {
                "_id": "$chauffeurs_cache.nom_chauffeur",
                "cumul_retard": {"$sum": "$total_retard"},
                "cumul_trajets": {"$sum": "$nb_trajets"},
            },
        },
        {
            "$project": {
                "_id": 0,
                "nom": "$_id",
                "moyenne_retard_minutes": {
                    "$cond": [
                        {"$eq": ["$cumul_trajets", 0]},
                        0,
                        {"$divide": ["$cumul_retard", "$cumul_trajets"]},
                    ],
                },
            },
        },
        {"$sort": {"moyenne_retard_minutes": -1}},
    ]

    df = aggregate_to_df(db.lignes, pipeline)
    return df[["nom", "moyenne_retard_minutes"]] if not df.empty else df


def query_L_mongo(db) -> pd.DataFrame:
    """
    Requête L (MongoDB) :
    Pour les lignes de type 'Bus', calcul du pourcentage de véhicules électriques.

    Retourne un DataFrame avec :
    - nom_ligne
    - total_vehicules
    - nb_electriques
    - pourcentage_electrique
    """
    pipeline = [
        {
            "$match": {
                "type": "Bus",
                "vehicules_cache": {"$exists": True, "$ne": []},
            },
        },
        {"$unwind": "$vehicules_cache"},
        {
            "$group": {
                "_id": "$nom_ligne",
                "total_vehicules": {"$sum": 1},
                "nb_electriques": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$vehicules_cache.type_vehicule", "Electrique"]},
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

    docs = list(db.lignes.aggregate(pipeline))
    if not docs:
        return pd.DataFrame(
            columns=[
                "nom_ligne",
                "total_vehicules",
                "nb_electriques",
                "pourcentage_electrique",
            ],
        )

    df = pd.DataFrame(docs)
    cols = [
        "nom_ligne",
        "total_vehicules",
        "nb_electriques",
        "pourcentage_electrique",
    ]
    return df[cols]


def query_M_mongo(db) -> pd.DataFrame:
    """
    Requête M (MongoDB) :
    Classification des capteurs CO2 par niveau de pollution.

    Retourne un DataFrame avec :
    - id_capteur
    - latitude
    - longitude
    - moyenne_co2
    - niveau_pollution
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
    cols = [
        "id_capteur",
        "latitude",
        "longitude",
        "moyenne_co2",
        "niveau_pollution",
    ]
    return df[cols] if not df.empty else df


def query_N_mongo(db) -> pd.DataFrame:
    """
    Requête N (MongoDB) :
    Catégorisation des lignes par niveau de fréquentation.

    Retourne un DataFrame avec :
    - nom_ligne
    - type
    - frequentation_moyenne
    - categorie_frequentation
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
    cols = [
        "nom_ligne",
        "type",
        "frequentation_moyenne",
        "categorie_frequentation",
    ]
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

@st.cache_data(show_spinner=False)
def executer_toutes_les_requetes_mongo() -> Dict[str, pd.DataFrame]:
    """
    Exécute toutes les requêtes MongoDB métier (A → N).
    UTILISE LE CACHE STREAMLIT.
    """
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
    resultats: Dict[str, pd.DataFrame] = {}

    try:
        client.admin.command("ping")
        if MONGO_DB_NAME not in client.list_database_names():
            error_df = pd.DataFrame([{"erreur": f"Base '{MONGO_DB_NAME}' inexistante."}])
            # On retourne une erreur pour toutes les clés pour éviter des plantages UI
            return {code: error_df for code in QUERY_MONGO_FUNCS.keys()}

        db = client[MONGO_DB_NAME]
        for code, func in QUERY_MONGO_FUNCS.items():
            try:
                # Calcul seulement
                df = func(db)
            except Exception as exc:
                df = pd.DataFrame([{"erreur": f"Erreur requête: {str(exc)}"}])
            resultats[code] = df

    except Exception as exc:
        err_msg = f"Impossible de se connecter à MongoDB : {str(exc)}"
        error_df = pd.DataFrame([{"erreur": err_msg}])
        return {code: error_df for code in QUERY_MONGO_FUNCS.keys()}
    finally:
        client.close()

    return resultats

def tenter_chargement_depuis_csv(dossier_cible: str) -> Dict[str, pd.DataFrame]:
    """
    Tente de recharger les DataFrames depuis les CSV existants sur le disque
    pour restaurer l'état de l'application au démarrage.
    """
    resultats_charges = {}
    # On parcourt les codes A, B, C... définis dans les objectifs
    for code in REQUETES_OBJECTIFS.keys():
        nom_fichier = f"resultat_req_{code.lower()}.csv"
        chemin_complet = os.path.join(dossier_cible, nom_fichier)
        
        if os.path.exists(chemin_complet):
            try:
                # On charge le CSV
                df = pd.read_csv(chemin_complet)
                resultats_charges[code] = df
            except Exception:
                # Si un fichier est corrompu, on l'ignore
                pass
    
    return resultats_charges

def forcer_ecriture_csv_mongo(resultats: Dict[str, pd.DataFrame]) -> None:
    """
    Fonction NON CACHÉE : Écrit les résultats Mongo sur le disque.
    """
    os.makedirs(DOSSIER_MONGO_CSV, exist_ok=True)
    print("💾 Sauvegarde CSV MongoDB sur disque...")
    for code, df in resultats.items():
        nom_fichier = f"resultat_req_{code.lower()}.csv"
        full_path = os.path.join(DOSSIER_MONGO_CSV, nom_fichier)
        try:
            df.to_csv(full_path, index=False, encoding="utf-8-sig")
        except Exception as e:
            print(f"Erreur écriture {nom_fichier}: {e}")
    print("✅ CSV MongoDB sauvegardés.")

def check_connexion_details() -> tuple[bool, bool]:
    """
    Vérifie la disponibilité du serveur MongoDB et l'existence de la base Paris2055.

    Retour
    ------
    (bool, bool)
        - premier booléen : True si le serveur MongoDB répond au ping.
        - second booléen : True si la base MONGO_DB_NAME est présente sur le serveur.
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
# ÉTAT GLOBAL STREAMLIT : DOSSIERS ET SESSION
# =====================================================================

# Création des répertoires nécessaires pour stocker la base SQLite,
# les exports CSV et les collections JSON.
os.makedirs(DOSSIER_DATA, exist_ok=True)
os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
os.makedirs(DOSSIER_CSV, exist_ok=True)
os.makedirs(DOSSIER_JSON, exist_ok=True)
os.makedirs(DOSSIER_MONGO_CSV, exist_ok=True)

def init_session_state() -> None:
    """
    Initialise les variables de session.
    Vérifie la présence de CSV sur le disque pour recharger l'état automatiquement.
    """
    if st.session_state.get("initialized", False):
        return

    if "groq_api_key" not in st.session_state:
        st.session_state["groq_api_key"] = os.getenv("GROQ_API_KEY", "")

    st.session_state["requetes_objectifs"] = REQUETES_OBJECTIFS

    # --- CHARGEMENT AUTOMATIQUE SQL ---
    if "resultats_sql" not in st.session_state:
        # On tente de charger depuis le disque
        data_sql = tenter_chargement_depuis_csv(DOSSIER_CSV)
        if data_sql:
            st.session_state["resultats_sql"] = data_sql
            st.session_state["queries_sql_executed"] = True
            print("✅ Session SQL restaurée depuis les CSV.")
        else:
            st.session_state["resultats_sql"] = {}
            st.session_state["queries_sql_executed"] = False

    # --- CHARGEMENT AUTOMATIQUE MONGO ---
    if "resultats_mongo" not in st.session_state:
        # On tente de charger depuis le disque
        data_mongo = tenter_chargement_depuis_csv(DOSSIER_MONGO_CSV)
        if data_mongo:
            st.session_state["resultats_mongo"] = data_mongo
            st.session_state["queries_mongo_executed"] = True
            print("✅ Session MongoDB restaurée depuis les CSV.")
        else:
            st.session_state["resultats_mongo"] = {}
            st.session_state["queries_mongo_executed"] = False

    # Autres états
    st.session_state["migration_logs"] = []
    st.session_state["migration_done_msg"] = ""
    st.session_state["migration_running"] = False
    st.session_state["ai_json_response"] = None
    st.session_state["ai_question_text_value"] = ""

    st.session_state["initialized"] = True

# =====================================================================
# LOGS MIGRATION (PARTIE 2) : STOCKAGE ET AFFICHAGE
# =====================================================================

MIGRATION_LOG_PLACEHOLDER: Optional[st.delta_generator.DeltaGenerator] = None
MAX_LOG_LINES = 300


def streamlit_migration_log(message: str, replace_last: bool = False) -> None:
    """
    Ajoute une entrée de log dans l'historique de migration et met à jour
    l'affichage Streamlit associé.

    Paramètres
    ----------
    message : str
        Ligne de log à ajouter ou à remplacer.
    replace_last : bool
        Si True, remplace la dernière ligne du log par ce message.
        Sinon, ajoute une nouvelle ligne à la fin.
    """
    global MIGRATION_LOG_PLACEHOLDER

    logs: List[str] = st.session_state.get("migration_logs", [])

    # Mise à jour de la liste en mémoire
    if replace_last and logs:
        logs[-1] = message
    else:
        logs.append(message)

    st.session_state["migration_logs"] = logs

    # Mise à jour de la zone de log en temps réel (mode "code" pour éviter les erreurs)
    if MIGRATION_LOG_PLACEHOLDER is not None:
        display_lines = logs[-MAX_LOG_LINES:]
        text_content = "\n".join(display_lines)
        MIGRATION_LOG_PLACEHOLDER.code(text_content, language="text")


# =====================================================================
# PARTIE 1 : INTERFACE REQUÊTES SQLITE
# =====================================================================
def render_partie_1_sqlite(tab) -> None:
    with tab:
        st.subheader("Partie 1 : Requêtes SQLite")

        status_text = "Données chargées." if st.session_state["queries_sql_executed"] else "Données non chargées."
        st.write(status_text)

        # Modification ici : Calcul + Sauvegarde
        if st.button("Executer & Sauvegarder CSV", key="btn_sql_run"):
            with st.spinner("Traitement en cours (Cache + Disque)..."):
                # 1. Récupération rapide (ou calcul)
                res = executer_toutes_les_requetes()
                
                # 2. Écriture forcée sur le disque
                forcer_ecriture_csv_sql(res)
                
                st.session_state["resultats_sql"] = res
                st.session_state["queries_sql_executed"] = True
                st.success("✅ Calculs terminés et CSV mis à jour sur le disque.")

        st.markdown("---")

        if not st.session_state["queries_sql_executed"]:
            st.info("Cliquez sur le bouton pour lancer les requêtes.")
            return

        st.markdown("### Résultats détaillés des requêtes SQLite")
        for code, objectif in st.session_state["requetes_objectifs"].items():
            df = st.session_state["resultats_sql"].get(code)
            with st.expander(f"Requête {code} – {objectif}", expanded=False):
                st.markdown(f"**Objectif :** {objectif}")
                if df is None:
                    st.warning("Aucun résultat.")
                elif df.empty:
                    st.info("Résultat vide.")
                else:
                    # --- CORRECTION ICI ---
                    st.dataframe(
                        df.style.set_properties(**{"text-align": "left"}), 
                        width="stretch"  # Remplace use_container_width=True
                    )

# =====================================================================
# PARTIE 2 : INTERFACE MIGRATION SQLITE → MONGODB
# =====================================================================

def render_partie_2_migration(tab) -> None:
    """
    Rend l'onglet 'Partie 2 : Migration vers MongoDB'.

    Cet onglet permet :
    - de déclencher la migration SQLite → modèle document → MongoDB ;
    - de suivre en temps réel un journal d'exécution (logs) ;
    - de consulter, après coup, le journal complet de la dernière migration.
    """
    global MIGRATION_LOG_PLACEHOLDER

    with tab:
        st.subheader("Partie 2 : Migration vers MongoDB")
        st.caption("Cliquez pour migrer les données de SQLite vers MongoDB.")

        def start_migration_callback() -> None:
            """
            Prépare l'état de session avant de lancer une nouvelle migration :
            - indique que la migration est en cours ;
            - vide le journal de logs ;
            - efface le message de fin précédent.
            """
            st.session_state["migration_running"] = True
            st.session_state["migration_logs"] = []
            st.session_state["migration_done_msg"] = ""

        col_btn, col_status = st.columns([1, 3], gap="small")

        # Bouton principal de lancement de la migration
        with col_btn:
            st.button(
                "Lancer Migration",
                key="btn_migration",
                width="content",
                on_click=start_migration_callback,
                disabled=st.session_state.get("migration_running", False),
            )

        # Zone dynamique où s'affichent les logs en temps réel ou en mode "historique"
        MIGRATION_LOG_PLACEHOLDER = st.empty()

        # Cas 1 : migration en cours, logs mis à jour en temps réel
        if st.session_state.get("migration_running", False):
            MIGRATION_LOG_PLACEHOLDER.code("Initialisation...", language="text")

            with col_status:
                with st.spinner("Traitement en cours..."):
                    migrer_sqlite_vers_mongo(log_fn_raw=streamlit_migration_log)

            st.session_state["migration_done_msg"] = "Migration terminée avec succès !"
            st.session_state["migration_running"] = False

            st.toast("Terminé !", icon="🎉")
            time.sleep(1)
            st.rerun()

        # Message de succès persistant après la fin de la migration
        if st.session_state.get("migration_done_msg"):
            with col_status:
                st.success(st.session_state["migration_done_msg"])

        # Cas 2 : migration terminée ou logs existants, affichage statique du journal
        logs: List[str] = st.session_state.get("migration_logs", [])
        if logs and not st.session_state.get("migration_running", False):
            log_text = "\n".join(logs[-MAX_LOG_LINES:])
            MIGRATION_LOG_PLACEHOLDER.text_area(
                "Journal de migration (Terminé)",
                value=log_text,
                height=400,
                key="final_log_view",
            )


# =====================================================================
# PARTIE 3 : INTERFACE REQUÊTES MONGODB
# =====================================================================
def render_partie_3_mongo(tab) -> None:
    with tab:
        st.subheader("Partie 3 : Requêtes MongoDB")

        server_ok, db_ok = check_connexion_details()

        if not server_ok:
            st.error(f"❌ Impossible de se connecter au serveur MongoDB sur {MONGO_URI}")
        elif not db_ok:
            st.warning(f"⚠️ La base '{MONGO_DB_NAME}' n'existe pas encore. Lancez la migration en Partie 2.")
        else:
            st.success(f"✅ Serveur connecté et base '{MONGO_DB_NAME}' détectée.")

        st.markdown("---")
        
        btn_disabled = not (server_ok and db_ok)

        # Modification ici : Calcul + Sauvegarde
        if st.button("Executer & Sauvegarder CSV Mongo", key="btn_mongo_run", disabled=btn_disabled):
            with st.spinner("Traitement en cours (Cache + Disque)..."):
                # 1. Récupération rapide
                res = executer_toutes_les_requetes_mongo()
                
                # 2. Écriture forcée sur disque
                forcer_ecriture_csv_mongo(res)
                
                st.session_state["resultats_mongo"] = res
                st.session_state["queries_mongo_executed"] = True
                st.success("✅ Requêtes MongoDB terminées et CSV sauvegardés.")

        if not st.session_state.get("queries_mongo_executed"):
            st.info("Cliquez sur le bouton pour lancer les requêtes.")
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
                    # --- CORRECTION ICI ---
                    st.dataframe(
                        df.style.set_properties(**{"text-align": "left"}), 
                        width="stretch"  # Remplace use_container_width=True
                    )
# =====================================================================
# PARTIE 4 : TABLEAU DE BORD / CARTOGRAPHIE (PLACEHOLDER)
# =====================================================================

def render_partie_4_streamlit(tab) -> None:
    """
    Rend l'onglet 'Partie 4 : Tableau de bord et cartographie'.

    Cet onglet est un espace réservé pour des visualisations avancées
    (cartes, graphiques, indicateurs métiers) construites à partir des
    données SQL ou MongoDB.
    """
    with tab:
        st.subheader("Partie 4 : Tableau de bord et cartographie")
        st.info(
            "Espace réservé pour des visualisations supplémentaires "
            "directement à partir des données de Paris 2055 "
            "(cartes, dashboards, etc.).",
        )


# =====================================================================
# PARTIE 5 : COMPARAISON SQL vs MONGODB
# =====================================================================

def comparer_dataframes_souple(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
) -> tuple[str, str]:
    """
    Compare deux DataFrames de manière tolérante afin d'évaluer la
    cohérence entre résultats SQL et MongoDB.

    La comparaison se fait en plusieurs étapes :
    - vérification de présence (aucun / un seul / les deux) ;
    - comparaison du nombre de lignes ;
    - comparaison du nombre de colonnes ;
    - tentative de comparaison des valeurs avec tolérance sur les types
      et les arrondis numériques.

    Retour
    ------
    (str, str)
        - icône de statut ("✅", "⚠️", "❌") ;
        - message explicatif en français.
    """
    if df1 is None or df2 is None:
        return "❌", "Un des résultats est manquant."

    if df1.empty and df2.empty:
        return "✅", "Les deux résultats sont vides (cohérent)."

    if df1.empty or df2.empty:
        return "❌", f"Disparité : SQL a {len(df1)} lignes, Mongo a {len(df2)} lignes."

    # 1. Nombre de lignes
    if len(df1) != len(df2):
        diff = abs(len(df1) - len(df2))
        return "⚠️", (
            f"Différence de taille : {len(df1)} (SQL) vs {len(df2)} (Mongo). "
            f"Écart : {diff}."
        )

    # 2. Nombre de colonnes
    if len(df1.columns) != len(df2.columns):
        return "⚠️", (
            f"Colonnes différentes : {list(df1.columns)} "
            f"vs {list(df2.columns)}."
        )

    # 3. Comparaison détaillée des valeurs avec tolérance sur les types et les arrondis
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
        return "✅", "Contenu identique (valeurs et dimensions)."
    except AssertionError:
        return (
            "⚠️",
            "Dimensions identiques, mais certaines valeurs diffèrent "
            "(arrondis, ordre ou types).",
        )
    except Exception as exc:
        return "❌", f"Erreur lors de la comparaison : {str(exc)}"


def render_partie_5_comparaison(tab) -> None:
    """
    Rend l'onglet 'Partie 5 : Validation de la Migration (SQL vs NoSQL)'.

    Cet onglet affiche, pour chaque requête A → N :
    - un statut de comparaison (identique / approximatif / incohérent) ;
    - les résultats SQL et MongoDB côte à côte ;
    - un score global de validation de la migration.
    """
    with tab:
        st.subheader("Partie 5 : Validation de la Migration (SQL vs NoSQL)")
        st.markdown(
            "Cet onglet permet de vérifier si les requêtes MongoDB renvoient "
            "bien les mêmes données métier que les requêtes SQL d'origine."
        )

        sql_ready = st.session_state.get("queries_sql_executed", False)
        mongo_ready = st.session_state.get("queries_mongo_executed", False)

        if not sql_ready or not mongo_ready:
            st.warning(
                "⚠️ Veuillez exécuter les requêtes de la **Partie 1** (SQL) "
                "et de la **Partie 3** (MongoDB) pour voir la comparaison.",
            )
            return

        st.markdown("---")

        res_sql = st.session_state["resultats_sql"]
        res_mongo = st.session_state["resultats_mongo"]
        objectifs = st.session_state["requetes_objectifs"]

        total_ok = 0
        total_queries = len(objectifs)

        # Expander par requête A → N
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
                    st.markdown("**1. Résultat SQL (Source)**")
                    if df_sql is not None and not df_sql.empty:
                        st.dataframe(df_sql, width="stretch", height=200)
                        st.caption(
                            f"Lignes : {len(df_sql)} | "
                            f"Colonnes : {len(df_sql.columns)}"
                        )
                    else:
                        st.info("Vide ou erreur.")

                with col_b:
                    st.markdown("**2. Résultat MongoDB (Cible)**")
                    if df_mongo is not None and not df_mongo.empty:
                        st.dataframe(df_mongo, width="stretch", height=200)
                        st.caption(
                            f"Lignes : {len(df_mongo)} | "
                            f"Colonnes : {len(df_mongo.columns)}"
                        )
                    else:
                        st.info("Vide ou erreur.")

        st.markdown("---")

        score = int((total_ok / total_queries) * 100)
        if score == 100:
            st.success(
                f"🏆 Migration validée à 100% ! "
                f"({total_ok}/{total_queries} requêtes identiques)",
            )
        elif score > 80:
            st.success(
                f"✅ Migration validée à {score}% "
                f"({total_ok}/{total_queries} requêtes identiques)",
            )
        else:
            st.error(
                f"❌ Attention : seulement {score}% de correspondance "
                f"({total_ok}/{total_queries}). Vérifiez vos pipelines.",
            )


# =====================================================================
# PARTIE 6 : ASSISTANT IA (GROQ / LLAMA3)
# =====================================================================

def interroger_groq(question: str) -> tuple[Optional[Dict], Optional[str]]:
    """
    Appelle l'API Groq (modèle Llama 3.3) pour générer un pipeline
    d'agrégation MongoDB à partir d'une question en langage naturel.

    Paramètres
    ----------
    question : str
        Question de l'utilisateur (en français) portant sur les données
        du modèle document Paris2055.

    Retour
    ------
    (dict | None, str | None)
        - objet JSON contenant au minimum : "collection" et "pipeline" ;
        - message d'erreur éventuel (None si pas d'erreur).
    """
    api_key = st.session_state.get("groq_api_key", "")

    if not api_key or "gsk_" not in api_key:
        return None, "Clé API Groq manquante ou invalide."

    client = Groq(api_key=api_key)

    try:
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {
                    "role": "system",
                    "content": (
                        SCHEMA_CONTEXT
                        + "\n\nIMPORTANT : Analyse bien la demande. "
                        "Si on cherche une moyenne ou un total, vérifie d'abord "
                        "si 'stats_trafic' ou 'co2_moyen_ligne' existent pour "
                        "optimiser. Réponds UNIQUEMENT au format JSON."
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
        return None, "La réponse de l'IA n'est pas un JSON valide."
    except Exception as exc:
        return None, str(exc)


def analyser_resultats_avec_ia(
    question_user: str,
    df: pd.DataFrame,
    api_key: str,
) -> str:
    """
    Demande à l'IA de produire une interprétation textuelle synthétique
    d'un DataFrame résultant d'une requête.

    L'analyse s'adapte au format des données :
    - une ligne unique → explication directe sans statistiques ;
    - plusieurs lignes → synthèse avec mise en avant des valeurs
      extrêmes et des tendances.

    Paramètres
    ----------
    question_user : str
        Question initiale posée par l'utilisateur.
    df : pandas.DataFrame
        Résultat tabulaire de la requête exécutée.
    api_key : str
        Clé API Groq à utiliser pour l'appel.

    Retour
    ------
    str
        Texte d'analyse en français, concis et orienté métier.
    """
    if df.empty:
        return "Je n'ai trouvé aucun résultat à analyser."

    nb_lignes = len(df)
    data_sample = df.head(10).to_string(index=False)

    stats_context = ""
    consigne_adaptative = ""

    # Cas 1 : une seule ligne → pas de statistiques globales
    if nb_lignes == 1:
        consigne_adaptative = (
            "Le résultat est une donnée UNIQUE (une seule ligne). "
            "Ne fais pas de statistiques (pas de min/max/moyenne). "
            "Explique simplement la valeur trouvée par rapport à la question."
        )
    else:
        # Cas 2 : plusieurs lignes → analyse statistique si possible
        nums = df.select_dtypes(include=["number"])
        if not nums.empty:
            try:
                stats_desc = nums.describe().to_string()
                stats_context = f"\nSTATISTIQUES DESCRIPTIVES (pour toi) :\n{stats_desc}\n"
                consigne_adaptative = (
                    "Le résultat contient plusieurs lignes. "
                    "Analyse les variations : cite les valeurs élevées et basses, "
                    "situe-les éventuellement par rapport à la moyenne, "
                    "et fais une synthèse plutôt qu'une liste brute."
                )
            except Exception:
                consigne_adaptative = (
                    "Résume les principaux éléments observables dans ce tableau."
                )
        else:
            consigne_adaptative = (
                "Ce résultat est essentiellement textuel. "
                "Fais une synthèse des éléments les plus importants."
            )

    prompt = (
        f"CONTEXTE : L'utilisateur a demandé : '{question_user}'.\n\n"
        f"DONNÉES (extrait) :\n{data_sample}\n"
        f"{stats_context}\n"
        f"CONSIGNE : Agis comme un analyste de données. {consigne_adaptative}\n"
        "Réponds en 2 phrases maximum, ton naturel et professionnel."
    )

    client = Groq(api_key=api_key)
    try:
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {
                    "role": "system",
                    "content": "Tu es un assistant analyste de données concis.",
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0.3,
        )
        return completion.choices[0].message.content
    except Exception as exc:
        return f"Analyse indisponible : {str(exc)}"


def render_partie_6_ia(tab) -> None:
    """
    Rend l'onglet 'Partie 6 : Assistant IA'.

    Cet onglet permet :
    - de poser une question métier en langage naturel ;
    - de laisser l'IA générer un pipeline MongoDB adapté ;
    - d'exécuter ce pipeline et d'afficher le résultat ;
    - de recevoir une interprétation textuelle des résultats.
    """
    QUESTION_BUTTONS = [
        "la moyenne des retards (en minutes) pour chaque ligne de transport.",
        "le nombre moyen de passagers transportés par jour pour chaque ligne.",
        "le taux d'incidents (en pourcentage) pour chaque ligne, basé sur le nombre de trajets ayant signalé un incident.",
        "les 5 quartiers ayant la moyenne de niveau de bruit (en dB) la plus élevée, basée sur les capteurs de bruit aux arrêts.",
    ]

    if "ai_json_response" not in st.session_state:
        st.session_state["ai_json_response"] = None
    if "question_a_traiter" not in st.session_state:
        st.session_state["question_a_traiter"] = ""

    def set_question(q: str) -> None:
        """Stocke directement une question prédéfinie dans l'état de session."""
        st.session_state["question_a_traiter"] = q

    def set_question_from_input() -> None:
        """Récupère la question saisie dans la zone de texte et la stocke."""
        st.session_state["question_a_traiter"] = st.session_state.get(
            "ai_question_input",
            "",
        )

    with tab:
        st.subheader("Partie 6 : Assistant IA 🧠 (Llama 3.3)")
        st.markdown(
            "Modèle utilisé : **llama-3.3-70b-versatile** "
            "(génération de pipelines MongoDB en JSON strict)."
        )

        # Zone de saisie principale
        st.text_area(
            "💬 Posez votre question :",
            key="ai_question_input",
            height=70,
        )
        st.button(
            "✨ Générer & Exécuter",
            type="primary",
            on_click=set_question_from_input,
        )

        results_container = st.container()

        # Boutons de questions pré-remplies
        st.markdown("---")
        st.caption("Questions rapides :")
        cols = st.columns(len(QUESTION_BUTTONS))
        for i, question_text in enumerate(QUESTION_BUTTONS):
            cols[i].button(
                question_text,
                key=f"quick_q_{i}",
                on_click=set_question,
                args=(question_text,),
            )

        # Logique de traitement de la question active
        question_actuelle = st.session_state["question_a_traiter"].strip()

        if question_actuelle:
            with results_container:
                st.info(f"Question : {question_actuelle}")

                # 1. Génération du pipeline MongoDB par l'IA
                with st.spinner("Génération de la requête MongoDB..."):
                    result_ia, error = interroger_groq(question_actuelle)

                st.session_state["ai_json_response"] = result_ia

                if st.session_state.get("ai_json_response"):
                    with st.expander(
                        "🛠️ Voir le JSON technique généré (Pipeline MongoDB)",
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
                    st.error(f"Erreur IA : {error}")
                    return

                collection_cible = result_ia.get("collection")
                pipeline = result_ia.get("pipeline")

                # Vérification minimale de la structure du pipeline
                if pipeline is None or not isinstance(pipeline, list):
                    st.error(
                        "❌ L'IA n'a pas généré un pipeline MongoDB valide "
                        "(structure inattendue).",
                    )
                    return

                # Cas défini dans SCHEMA_CONTEXT pour les questions hors sujet
                if len(pipeline) == 0:
                    st.warning(
                        "🤖 La question semble hors sujet par rapport à la base "
                        "de données. Reformulez votre demande.",
                    )
                    return

                # 2. Exécution du pipeline sur la collection cible
                with st.spinner(f"Exécution sur '{collection_cible}'..."):
                    try:
                        client = pymongo.MongoClient(MONGO_URI)
                        db = client[MONGO_DB_NAME]

                        if collection_cible not in db.list_collection_names():
                            st.error(
                                f"Erreur : Collection '{collection_cible}' introuvable.",
                            )
                            client.close()
                            return

                        results = list(db[collection_cible].aggregate(pipeline))
                        client.close()

                        if results:
                            st.markdown(f"### 📊 Résultats ({len(results)})")
                            df_res = pd.DataFrame(results)
                            if "_id" in df_res.columns:
                                df_res["_id"] = df_res["_id"].astype(str)

                            st.dataframe(df_res, width="stretch")

                            # 3. Analyse textuelle des résultats par l'IA
                            st.markdown("### 💡 Analyse")
                            with st.spinner("Analyse des résultats..."):
                                analyse = analyser_resultats_avec_ia(
                                    question_actuelle,
                                    df_res,
                                    st.session_state["groq_api_key"],
                                )
                            st.info(analyse, icon="📈")
                        else:
                            st.warning(
                                "La requête est valide, mais elle ne retourne "
                                "aucun document (tableau vide).",
                            )

                    except Exception as exc:
                        st.error(f"Erreur Mongo : {exc}")


# =====================================================================
# POINT D'ENTRÉE STREAMLIT
# =====================================================================
def main() -> None:
    """
    Point d'entrée principal de l'application.
    """
    st.set_page_config(
        page_title="Paris 2055 - Requêtes et Migration vers MongoDB",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    init_session_state()

    st.title("Paris 2055 - Requêtes et Migration vers MongoDB")

    with st.sidebar:
        st.header("📡 État du Système")
        sqlite_exists = os.path.exists(DB_FILE)
        sqlite_icon = "✅" if sqlite_exists else "❌"
        sqlite_msg = "Ready" if sqlite_exists else "Missing"

        mongo_status = "Disconnected"
        mongo_icon = "❌"
        mongo_color = "red"

        try:
            client_check = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=500)
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
        st.header("🗂️ État des Caches")
        if st.session_state.get("queries_sql_executed", False):
            st.success("Cache SQL : **Chargé**", icon="✅")
        else:
            st.info("Cache SQL : **Vide**", icon="⚪")

        if st.session_state.get("queries_mongo_executed", False):
            st.success("Cache Mongo : **Chargé**", icon="✅")
        else:
            st.info("Cache Mongo : **Vide**", icon="⚪")

        st.markdown("---")
        st.header("🔑 Config API")
        new_key = st.text_input("Groq API Key", value=st.session_state["groq_api_key"], type="password")
        if new_key != st.session_state["groq_api_key"]:
            st.session_state["groq_api_key"] = new_key
            os.environ["GROQ_API_KEY"] = new_key
            try:
                set_key(".env", "GROQ_API_KEY", new_key)
                st.success("Sauvegardé ! ✅")
            except: pass
            time.sleep(0.5)
            st.rerun()

        st.markdown("---")
        st.subheader("🧨 Danger Zone")

        # Bouton Optimisé : Supprime la DB, les CSV ET vide le cache RAM
        if st.button("🗑️ DROP DB, CSV & CLEAR CACHE", type="primary", use_container_width=True):
            try:
                # 1. Mongo Drop
                client = pymongo.MongoClient(MONGO_URI)
                client.drop_database(MONGO_DB_NAME)
                client.close()

                # 2. Delete CSVs
                for folder in [DOSSIER_CSV, DOSSIER_MONGO_CSV]:
                    if os.path.exists(folder):
                        for filename in os.listdir(folder):
                            file_path = os.path.join(folder, filename)
                            try:
                                if os.path.isfile(file_path): os.unlink(file_path)
                            except Exception as e: print(f"Erreur suppression {file_path}: {e}")

                # 3. CLEAR STREAMLIT CACHE
                st.cache_data.clear()

                # 4. Reset Session
                st.session_state["queries_mongo_executed"] = False
                st.session_state["resultats_mongo"] = {}
                st.session_state["queries_sql_executed"] = False
                st.session_state["resultats_sql"] = {}
                st.session_state["migration_logs"] = []
                st.session_state["migration_running"] = False
                st.session_state["migration_done_msg"] = ""
                st.session_state["ai_json_response"] = None

                st.toast("Tout est nettoyé (RAM + Disque) !", icon="💥")
                time.sleep(1.5)
                st.rerun()
            except Exception as exc:
                st.error(f"Erreur : {exc}")

    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "Partie 1 : Requêtes SQL", "Partie 2 : Migration", "Partie 3 : Requêtes MongoDB",
        "Partie 4 : Dashboard", "Partie 5 : Comparaison", "Partie 6 : Assistant IA"
    ])

    render_partie_1_sqlite(tab1)
    render_partie_2_migration(tab2)
    render_partie_3_mongo(tab3)
    render_partie_4_streamlit(tab4)
    render_partie_5_comparaison(tab5)
    render_partie_6_ia(tab6)

if __name__ == "__main__":
    main()
