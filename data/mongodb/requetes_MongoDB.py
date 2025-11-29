import pandas as pd
from typing import List, Dict, Callable

# --- Utilitaires ---

def aggregate_to_df(collection, pipeline: List[Dict]) -> pd.DataFrame:
    """
    Exécute un pipeline d'agrégation MongoDB et renvoie le résultat
    sous forme de DataFrame.
    """
    documents = list(collection.aggregate(pipeline))
    if not documents:
        return pd.DataFrame()
    return pd.DataFrame(documents)

# --- Fonctions de requêtes ---

def query_A_mongo(db) -> pd.DataFrame:
    # ... (Copie ici ta fonction query_A_mongo complète) ...
    pipeline = [
        {"$unwind": "$trafic"},
        {"$match": {"trafic.retard_minutes": {"$ne": None}}},
        {"$group": {"_id": "$nom_ligne", "moyenne_retard_minutes": {"$avg": "$trafic.retard_minutes"}}},
        {"$sort": {"moyenne_retard_minutes": -1}},
        {"$project": {"_id": 0, "nom_ligne": "$_id", "moyenne_retard_minutes": 1}},
    ]
    df = aggregate_to_df(db.lignes, pipeline)
    return df[["nom_ligne", "moyenne_retard_minutes"]] if not df.empty else df

def query_B_mongo(db) -> pd.DataFrame:
    # ... (Copie ici le corps de ta fonction query_B_mongo) ...
    # Pour gagner de la place dans la réponse, je te laisse coller ton code exact ici
    # Assure-toi juste que chaque fonction appelle aggregate_to_df défini plus haut.
    # Ci-dessous, je remets le code exact que tu as fourni pour B à N :
    pipeline = [
        {"$unwind": "$arrets"},
        {"$unwind": "$arrets.horaires"},
        {
            "$project": {
                "_id": 0,
                "id_ligne": "$id_ligne",
                "jour": {"$substrBytes": ["$arrets.horaires.heure_prevue", 0, 10]},
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
    pipeline = [
        {"$unwind": {"path": "$trafic", "preserveNullAndEmptyArrays": True}},
        {"$group": {
            "_id": {"id_ligne": "$id_ligne", "nom_ligne": "$nom_ligne", "id_trafic": "$trafic.id_trafic"},
            "has_incident": {"$max": {"$cond": [{"$gt": [{"$size": {"$ifNull": ["$trafic.incidents", []]}}, 0]}, 1, 0]}}
        }},
        {"$group": {
            "_id": "$_id.nom_ligne",
            "nb_trafic_avec_incident": {"$sum": "$has_incident"},
            "nb_total_trafic": {"$sum": {"$cond": [{"$ifNull": ["$_id.id_trafic", False]}, 1, 0]}}
        }},
        {"$addFields": {"taux_incident_pourcent": {"$cond": [{"$eq": ["$nb_total_trafic", 0]}, 0, {"$multiply": [{"$divide": ["$nb_trafic_avec_incident", "$nb_total_trafic"]}, 100]}]}}},
        {"$sort": {"taux_incident_pourcent": -1}},
        {"$project": {"_id": 0, "nom_ligne": "$_id", "nb_trafic_avec_incident": 1, "nb_total_trafic": 1, "taux_incident_pourcent": 1}}
    ]
    df = aggregate_to_df(db.lignes, pipeline)
    return df[["nom_ligne", "nb_trafic_avec_incident", "nb_total_trafic", "taux_incident_pourcent"]] if not df.empty else df

def query_D_mongo(db) -> pd.DataFrame:
    pipeline = [
        {"$match": {"co2_moyen_ligne": {"$exists": True, "$ne": None}, "vehicules_cache": {"$exists": True, "$ne": []}}},
        {"$project": {"vehicules_cache": 1, "co2_moyen_ligne": 1}},
        {"$unwind": "$vehicules_cache"},
        {"$project": {"_id": 0, "id_vehicule": "$vehicules_cache.id_vehicule", "immatriculation": "$vehicules_cache.immatriculation", "moyenne_co2": "$co2_moyen_ligne"}},
        {"$sort": {"moyenne_co2": -1}},
    ]
    df = aggregate_to_df(db.lignes, pipeline)
    return df[["id_vehicule", "immatriculation", "moyenne_co2"]] if not df.empty else pd.DataFrame(columns=["id_vehicule", "immatriculation", "moyenne_co2"])

def query_E_mongo(db) -> pd.DataFrame:
    pipeline = [
        {"$lookup": {"from": "capteurs", "localField": "arrets.id_arret", "foreignField": "arret.id_arret", "as": "caps"}},
        {"$unwind": "$caps"},
        {"$match": {"caps.type_capteur": "Bruit"}},
        {"$unwind": "$caps.mesures"},
        {"$group": {"_id": "$nom", "moyenne_bruit_db": {"$avg": "$caps.mesures.valeur"}}},
        {"$sort": {"moyenne_bruit_db": -1}},
        {"$limit": 5},
        {"$project": {"_id": 0, "nom": "$_id", "moyenne_bruit_db": 1}}
    ]
    df = aggregate_to_df(db.quartiers, pipeline)
    return df[["nom", "moyenne_bruit_db"]] if not df.empty else df

def query_F_mongo(db) -> pd.DataFrame:
    pipeline = [
        {"$project": {
            "nom_ligne": 1,
            "has_big_delay": {"$gt": [{"$size": {"$filter": {"input": {"$ifNull": ["$trafic", []]}, "as": "t", "cond": {"$gt": ["$$t.retard_minutes", 10]}}}}, 0]},
            "has_incident": {"$gt": [{"$size": {"$filter": {"input": {"$ifNull": ["$trafic", []]}, "as": "t", "cond": {"$gt": [{"$size": {"$ifNull": ["$$t.incidents", []]}}, 0]}}}}, 0]}
        }},
        {"$match": {"has_big_delay": True, "has_incident": False}},
        {"$project": {"_id": 0, "nom_ligne": 1}}
    ]
    df = aggregate_to_df(db.lignes, pipeline)
    return df[["nom_ligne"]] if not df.empty else df

def query_G_mongo(db) -> pd.DataFrame:
    pipeline = [
        {"$unwind": "$trafic"},
        {"$group": {"_id": None, "total_trajets": {"$sum": 1}, "trajets_sans_retard": {"$sum": {"$cond": [{"$eq": ["$trafic.retard_minutes", 0]}, 1, 0]}}}},
        {"$addFields": {"taux_ponctualite_global_pourcent": {"$cond": [{"$eq": ["$total_trajets", 0]}, 0, {"$multiply": [{"$divide": ["$trajets_sans_retard", "$total_trajets"]}, 100]}]}}},
        {"$project": {"_id": 0, "total_trajets": 1, "trajets_sans_retard": 1, "taux_ponctualite_global_pourcent": 1}}
    ]
    df = aggregate_to_df(db.lignes, pipeline)
    return df[["total_trajets", "trajets_sans_retard", "taux_ponctualite_global_pourcent"]] if not df.empty else df

def query_H_mongo(db) -> pd.DataFrame:
    pipeline = [
        {"$addFields": {"nombre_arrets": {"$size": {"$ifNull": ["$arrets", []]}}}},
        {"$sort": {"nombre_arrets": -1}},
        {"$project": {"_id": 0, "nom": 1, "nombre_arrets": 1}}
    ]
    df = aggregate_to_df(db.quartiers, pipeline)
    return df[["nom", "nombre_arrets"]] if not df.empty else df

def query_I_mongo(db) -> pd.DataFrame:
    pipeline = [
        {"$addFields": {"moyenne_retard": {"$cond": [{"$gt": [{"$size": {"$ifNull": ["$trafic", []]}}, 0]}, {"$avg": "$trafic.retard_minutes"}, 0]}}},
        {"$lookup": {"from": "capteurs", "localField": "id_ligne", "foreignField": "arret.id_ligne", "as": "capteurs_ligne"}},
        {"$addFields": {"mesures_co2": {"$reduce": {"input": "$capteurs_ligne", "initialValue": [], "in": {"$cond": [{"$eq": ["$$this.type_capteur", "CO2"]}, {"$concatArrays": ["$$value", {"$map": {"input": {"$ifNull": ["$$this.mesures", []]}, "as": "m", "in": "$$m.valeur"}}]}, "$$value"]}}}}},
        {"$addFields": {"moyenne_co2": {"$cond": [{"$gt": [{"$size": "$mesures_co2"}, 0]}, {"$avg": "$mesures_co2"}, 0]}}},
        {"$sort": {"nom_ligne": 1}},
        {"$project": {"_id": 0, "nom_ligne": 1, "moyenne_retard": 1, "moyenne_co2": 1}}
    ]
    df = aggregate_to_df(db.lignes, pipeline)
    return df[["nom_ligne", "moyenne_retard", "moyenne_co2"]] if not df.empty else df

def query_J_mongo(db) -> pd.DataFrame:
    pipeline = [
        {"$match": {"type_capteur": "Temperature"}},
        {"$unwind": "$mesures"},
        {"$group": {"_id": "$arret.id_ligne", "moyenne_temperature": {"$avg": "$mesures.valeur"}}},
        {"$lookup": {"from": "lignes", "localField": "_id", "foreignField": "id_ligne", "as": "ligne"}},
        {"$unwind": "$ligne"},
        {"$sort": {"moyenne_temperature": -1}},
        {"$project": {"_id": 0, "nom_ligne": "$ligne.nom_ligne", "moyenne_temperature": 1}}
    ]
    df = aggregate_to_df(db.capteurs, pipeline)
    return df[["nom_ligne", "moyenne_temperature"]] if not df.empty else df

def query_K_mongo(db) -> pd.DataFrame:
    pipeline = [
        {"$match": {"stats_trafic": {"$exists": True}, "chauffeurs_cache": {"$exists": True, "$ne": []}}},
        {"$project": {"chauffeurs_cache": 1, "total_retard": "$stats_trafic.total_retard", "nb_trajets": "$stats_trafic.nb_trajets"}},
        {"$unwind": "$chauffeurs_cache"},
        {"$group": {"_id": "$chauffeurs_cache.nom_chauffeur", "cumul_retard": {"$sum": "$total_retard"}, "cumul_trajets": {"$sum": "$nb_trajets"}}},
        {"$project": {"_id": 0, "nom": "$_id", "moyenne_retard_minutes": {"$cond": [{"$eq": ["$cumul_trajets", 0]}, 0, {"$divide": ["$cumul_retard", "$cumul_trajets"]}]}}},
        {"$sort": {"moyenne_retard_minutes": -1}}
    ]
    df = aggregate_to_df(db.lignes, pipeline)
    return df[["nom", "moyenne_retard_minutes"]] if not df.empty else df

def query_L_mongo(db) -> pd.DataFrame:
    pipeline = [
        {"$match": {"type": "Bus", "vehicules_cache": {"$exists": True, "$ne": []}}},
        {"$unwind": "$vehicules_cache"},
        {"$group": {"_id": "$nom_ligne", "total_vehicules": {"$sum": 1}, "nb_electriques": {"$sum": {"$cond": [{"$eq": ["$vehicules_cache.type_vehicule", "Electrique"]}, 1, 0]}}}},
        {"$addFields": {"pourcentage_electrique": {"$cond": [{"$eq": ["$total_vehicules", 0]}, 0, {"$multiply": [{"$divide": ["$nb_electriques", "$total_vehicules"]}, 100]}]}}},
        {"$sort": {"pourcentage_electrique": -1}},
        {"$project": {"_id": 0, "nom_ligne": "$_id", "total_vehicules": 1, "nb_electriques": 1, "pourcentage_electrique": 1}}
    ]
    docs = list(db.lignes.aggregate(pipeline))
    if not docs:
        return pd.DataFrame(columns=["nom_ligne", "total_vehicules", "nb_electriques", "pourcentage_electrique"])
    df = pd.DataFrame(docs)
    return df[["nom_ligne", "total_vehicules", "nb_electriques", "pourcentage_electrique"]]

def query_M_mongo(db) -> pd.DataFrame:
    pipeline = [
        {"$match": {"type_capteur": "CO2"}},
        {"$unwind": "$mesures"},
        {"$group": {"_id": {"id_capteur": "$id_capteur", "position": "$position"}, "moyenne_co2": {"$avg": "$mesures.valeur"}}},
        {"$addFields": {"niveau_pollution": {"$switch": {"branches": [{"case": {"$gt": ["$moyenne_co2", 800]}, "then": "Élevé"}, {"case": {"$gt": ["$moyenne_co2", 450]}, "then": "Moyen"}], "default": "Faible"}}}},
        {"$sort": {"moyenne_co2": -1}},
        {"$project": {"_id": 0, "id_capteur": "$_id.id_capteur", "latitude": {"$arrayElemAt": ["$_id.position.coordinates", 1]}, "longitude": {"$arrayElemAt": ["$_id.position.coordinates", 0]}, "moyenne_co2": 1, "niveau_pollution": 1}}
    ]
    df = aggregate_to_df(db.capteurs, pipeline)
    return df[["id_capteur", "latitude", "longitude", "moyenne_co2", "niveau_pollution"]] if not df.empty else df

def query_N_mongo(db) -> pd.DataFrame:
    pipeline = [
        {"$addFields": {"categorie_frequentation": {"$switch": {"branches": [{"case": {"$gt": ["$frequentation_moyenne", 2000]}, "then": "Haute Fréquentation"}, {"case": {"$gt": ["$frequentation_moyenne", 1000]}, "then": "Moyenne Fréquentation"}], "default": "Basse Fréquentation"}}}},
        {"$sort": {"frequentation_moyenne": -1}},
        {"$project": {"_id": 0, "nom_ligne": 1, "type": 1, "frequentation_moyenne": 1, "categorie_frequentation": 1}}
    ]
    df = aggregate_to_df(db.lignes, pipeline)
    return df[["nom_ligne", "type", "frequentation_moyenne", "categorie_frequentation"]] if not df.empty else df

# --- Export des fonctions pour app.py ---

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