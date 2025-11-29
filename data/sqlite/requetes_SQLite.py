import sqlite3
import os
import pandas as pd
from typing import Dict

# Chemin relatif depuis la racine du projet
# On suppose que le script est lancé depuis la racine du projet (là où est app.py)
DB_FILE = os.path.join("data", "sqlite", "db", "paris2055.sqlite")

def executer_requetes_sql_module() -> Dict[str, pd.DataFrame]:
    """
    Exécute toutes les requêtes SQL métier (A → N) sur la base SQLite.
    """
    if not os.path.exists(DB_FILE):
        # Sécurité si le fichier n'existe pas encore
        return {}

    resultats: Dict[str, pd.DataFrame] = {}

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
                df_result = pd.read_sql_query(query, conn)
                resultats[code] = df_result
            except Exception as exc:
                resultats[code] = pd.DataFrame([{"erreur": str(exc)}])

    return resultats