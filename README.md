# Paris 2055 : ETL & Analytics MongoDB 🚇

[![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://streamlit.io/)
[![Python](https://img.shields.io/badge/Python-3.10+-blue?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![MongoDB](https://img.shields.io/badge/MongoDB-4EA94B?style=for-the-badge&logo=mongodb&logoColor=white)](https://www.mongodb.com/)
[![Groq AI](https://img.shields.io/badge/AI-Groq_Llama3.3-orange?style=for-the-badge)](https://groq.com/)

> **Projet pédagogique** - BUT 3 Sciences des Données (2025)

---

## 📖 Description du Projet

**Paris 2055** est une application complète d'**ETL** (Extract, Transform, Load) et d'**analyse de données** qui simule la migration d'un système de transport urbain futuriste depuis une architecture **relationnelle (SQLite)** vers une architecture **orientée documents (MongoDB)**.

### Contexte Métier

En 2055, le réseau de transport parisien génère des millions de données en temps réel :
- 📊 Mesures de capteurs environnementaux (**CO2**, **Bruit**, **Température**)
- 🚌 Informations de **trafic** (retards, incidents, fréquentation)
- 🗺️ Données **géospatiales** (quartiers, arrêts, positions des capteurs)
- 👨‍✈️ Données opérationnelles (chauffeurs, véhicules, horaires)

### Objectif Technique

Migrer les données depuis un modèle **relationnel normalisé** (14 tables SQLite) vers un modèle **dénormalisé orienté documents** (3 collections MongoDB) pour :
- ✅ Supporter la **volumétrie** croissante (millions de mesures)
- ✅ Optimiser les **requêtes analytiques** complexes
- ✅ Tirer parti de **MongoDB** (agrégations, géospatial, flexibilité du schéma)
- ✅ Intégrer un **assistant IA** pour générer des requêtes en langage naturel

---

## 🛠️ Technologies & Bibliothèques

| Technologie | Version | Utilisation |
|-------------|---------|-------------|
| **Python** | 3.10+ | Langage principal |
| **Streamlit** | 1.x | Interface web interactive (6 onglets) |
| **Pandas** | 2.x | Manipulation de DataFrames pour l'ETL |
| **PyMongo** | 4.x | Client MongoDB (connexion, requêtes, agrégations) |
| **SQLite3** | (std lib) | Accès à la base relationnelle source |
| **Groq SDK** | Latest | API Llama 3.3-70B pour l'assistant IA |
| **python-dotenv** | Latest | Gestion des variables d'environnement (.env) |

---

## 📦 Installation

### Prérequis

1. **Python 3.10 ou supérieur**
   ```bash
   python --version  # Vérifiez votre version
   ```

2. **MongoDB** (local ou Atlas)
   - **Option A (Local)** : Installez MongoDB Community Server
   - **Option B (Cloud)** : Créez un cluster gratuit sur [MongoDB Atlas](https://www.mongodb.com/cloud/atlas)

3. **Clé API Groq** (pour l'assistant IA)
   - Créez un compte sur [console.groq.com](https://console.groq.com/)
   - Générez une clé API (format : `gsk_...`)

### Étapes d'Installation

```bash
# 1. Cloner le dépôt (ou télécharger le projet)
git clone https://github.com/qurnt1/mongoDB_ST_QC_CA_JF.git
cd mongoDB_ST_QC_CA_JF

# 2. Créer un environnement virtuel (recommandé)
python -m venv venv

# 3. Activer l'environnement virtuel
.\venv\Scripts\activate

# 4. Créer le fichier .env
@("GROQ_API_KEY=''", "MONGO_URI=''") | Set-Content .env

# 5. Installer les dépendances
pip install -r requirements.txt
```

**macOs, Linux**

```bash
# 1. Cloner le dépôt (ou télécharger le projet)
git clone https://github.com/qurnt1/mongoDB_ST_QC_CA_JF.git
cd mongoDB_ST_QC_CA_JF

# 2. Créer un environnement virtuel (recommandé)
python -m venv venv

# 3. Activer l'environnement virtuel
source venv/bin/activate

# 4. Créer le fichier .env
echo "GROQ_API_KEY=''" > .env && echo "MONGO_URI=''" >> .env

# 5. Installer les dépendances
pip install -r requirements.txt
```

### Configuration (.env)

Modifiez / créez le fichier `.env` à la racine du projet :

```env
# Clé API Groq (obligatoire pour l'assistant IA - Partie 6)
GROQ_API_KEY="gsk_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

# URI MongoDB (optionnel, valeur par défaut : mongodb://127.0.0.1:27017/)
# Exemples :
# Local : MONGO_URI="mongodb://localhost:27017/"
# Atlas : MONGO_URI="mongodb+srv://user:password@cluster.mongodb.net/"
MONGO_URI="mongodb://127.0.0.1:27017/"
```

> **Note** : L'URI MongoDB peut également être modifiée directement dans l'interface Streamlit (sidebar → Config API & DB).

---

## 🏗️ Architecture

### Schéma de Données Source (SQLite)

La base `paris2055.sqlite` contient **14 tables relationnelles** :

| Table | Description | Clés Principales |
|-------|-------------|------------------|
| `Ligne` | Lignes de transport (Bus, Métro, Tramway) | `id_ligne`, `nom_ligne`, `type` |
| `Arret` | Arrêts de transport | `id_arret`, `id_ligne`, `latitude`, `longitude` |
| `Horaire` | Passages prévus | `id_horaire`, `id_arret`, `id_vehicule`, `heure_prevue`, `passagers_estimes` |
| `Trafic` | Événements de circulation | `id_trafic`, `id_ligne`, `retard_minutes` |
| `Incident` | Incidents signalés | `id_incident`, `id_trafic`, `description`, `gravite` |
| `Capteur` | Capteurs environnementaux | `id_capteur`, `id_arret`, `type_capteur`, `latitude`, `longitude` |
| `Mesure` | Relevés de capteurs | `id_mesure`, `id_capteur`, `valeur`, `horodatage`, `unite` |
| `Quartier` | Quartiers parisiens | `id_quartier`, `nom`, `geojson` (polygone) |
| `ArretQuartier` | Relation arrêt-quartier | `id_arret`, `id_quartier` |
| `Vehicule` | Flotte de véhicules | `id_vehicule`, `id_ligne`, `id_chauffeur`, `type_vehicule`, `immatriculation` |
| `Chauffeur` | Conducteurs | `id_chauffeur`, `nom` |

### Processus ETL (Extract, Transform, Load)

#### 1. **Extract** - Chargement optimisé depuis SQLite

```python
# Fonction : load_tables()
# Optimisation : Chargement par CHUNKS pour éviter les erreurs "Out of Memory"
```

- **Tables petites** (`Ligne`, `Arret`, `Quartier`, `Vehicule`, etc.) : Chargement standard
- **Tables volumineuses** (`Horaire`, `Mesure`, `Trafic`, `Incident`) : Chargement par chunks de 50 000 lignes

#### 2. **Transform** - Construction du modèle document

```python
# Fonctions : build_lignes_docs(), build_quartiers_docs(), build_capteurs_docs()
# Optimisation : Utilisation de itertuples() au lieu de iterrows() (10x plus rapide)
```

**Stratégies de transformation :**

- **Dénormalisation** : Regroupement des données liées en sous-documents imbriqués
- **Caches pré-calculés** : Ajout de champs dérivés pour optimiser les requêtes :
  - `vehicules_cache` : Liste des véhicules de la ligne (évite un $lookup)
  - `chauffeurs_cache` : Liste des chauffeurs affectés à la ligne
  - `stats_trafic` : Statistiques pré-agrégées (total_retard, nb_trajets, moyenne_precalc)
  - `co2_moyen_ligne` : Moyenne de CO2 calculée par ligne

**Exemple de transformation :**

```
SQLite (Modèle Relationnel)          MongoDB (Modèle Document)
┌──────────┐                         ┌──────────────────────────────┐
│  Ligne   │                         │         lignes               │
├──────────┤                         ├──────────────────────────────┤
│ id_ligne │──┐                      │ id_ligne                     │
│ nom      │  │                      │ nom_ligne                    │
│ type     │  │                      │ type                         │
└──────────┘  │                      │ co2_moyen_ligne (cache)      │
              │                      │ stats_trafic: {              │
              │                      │   total_retard,              │
┌──────────┐  │                      │   nb_trajets,                │
│  Arret   │──┘                      │   moyenne_precalc            │
├──────────┤                         │ }                            │
│ id_arret │──┐                      │ arrets: [                    │
│ nom      │  │                      │   {                          │
│ lat/lon  │  │                      │     id_arret,                │
└──────────┘  │                      │     nom,                     │
              │                      │     latitude,                │
┌──────────┐  │                      │     longitude,               │
│ Horaire  │──┘  ==TRANSFORM==>     │     horaires: [              │
├──────────┤                         │       { heure_prevue,        │
│ heure    │                         │         passagers_estimes }  │
│ passagers│                         │     ],                       │
└──────────┘                         │     capteurs_ids: [12, 34]   │
                                     │   }                          │
                                     │ ],                           │
                                     │ trafic: [                    │
                                     │   { retard_minutes,          │
                                     │     incidents: [...] }       │
                                     │ ]                            │
                                     └──────────────────────────────┘
```

#### 3. **Load** - Insertion dans MongoDB

```python
# Fonction : migrer_sqlite_vers_mongo()
# Optimisation : Insertion par batch de 25 000 documents (insert_many)
```

**Collections MongoDB créées :**

| Collection | Documents | Structure Principale |
|------------|-----------|---------------------|
| `lignes` | ~100 docs | Lignes avec arrêts, horaires, trafic, incidents, caches |
| `quartiers` | ~100 docs | Quartiers avec géométrie GeoJSON (Polygon) et arrêts |
| `capteurs` | ~4630 docs | Capteurs avec position GeoJSON (Point) et mesures |

**Index créés automatiquement :**

```javascript
// Index standards
db.lignes.createIndex({ id_ligne: 1 })
db.lignes.createIndex({ nom_ligne: 1 })
db.capteurs.createIndex({ type_capteur: 1 })

// Index géospatiaux (2dsphere) pour les requêtes de proximité
db.quartiers.createIndex({ geom: "2dsphere" })
db.capteurs.createIndex({ position: "2dsphere" })
```

---

## ▶️ Utilisation

### Lancement de l'Application

```bash
streamlit run app.py
```

L'application s'ouvre automatiquement dans votre navigateur à l'adresse **http://localhost:8501**

### Interface Streamlit (6 Onglets)

#### **Partie 1 : Requêtes SQL (Legacy)**
- Exécution de **14 requêtes métier** sur la base SQLite source
- Requêtes couvrant : moyennes de retards, taux d'incidents, fréquentation, émissions CO2, niveaux sonores, etc.
- Export des résultats au format **CSV** (cache persistant)

#### **Partie 2 : ETL & Migration**
- Bouton **"Lancer Migration"** pour déclencher le processus complet :
  1. Nettoyage des anciennes collections MongoDB
  2. Chargement des tables SQLite (optimisé par chunks)
  3. Construction des documents (avec caches et transformations)
  4. Sauvegarde JSON locale (backup)
  5. Insertion dans MongoDB (par batch)
  6. Création des index
- **Logs en temps réel** affichant la progression

#### **Partie 3 : Analytics NoSQL**
- Exécution de **14 requêtes équivalentes** sur MongoDB
- Utilisation de **pipelines d'agrégation** ($match, $group, $unwind, $lookup, etc.)
- Exploitation des **caches pré-calculés** (vehicules_cache, stats_trafic)
- Export CSV des résultats

#### **Partie 4 : Tableau de Bord**
- Espace réservé pour des visualisations avancées (cartes, graphiques interactifs)

#### **Partie 5 : Validation & Comparaison**
- Comparaison automatisée **SQL vs MongoDB** (ligne par ligne)
- Scoring de validation (pourcentage de requêtes identiques)
- Affichage côte à côte des résultats pour chaque requête

#### **Partie 6 : Assistant IA 🧠**
- **Modèle utilisé** : Llama 3.3-70B Versatile (via API Groq)
- **Fonctionnalités** :
  - Posez une question en **langage naturel** (français)
  - L'IA génère un **pipeline MongoDB** en JSON
  - Exécution automatique du pipeline
  - Analyse textuelle des résultats par l'IA
- **Exemples de questions** :
  - _"Quelle est la moyenne des retards pour chaque ligne de transport ?"_
  - _"Quels sont les 5 quartiers les plus bruyants ?"_
  - _"Classe les capteurs CO2 par niveau de pollution"_

---

## 🚦 Parcours Utilisateur Recommandé

1. **Vérifier l'état du système** (sidebar)
   - SQLite : ✅ Ready
   - MongoDB : 🚫 Offline/Empty

2. **Partie 1** : Exécuter les requêtes SQL pour générer les données de référence

3. **Partie 2** : Lancer la migration SQLite → MongoDB
   - Durée estimée : 30-60 secondes (selon la machine)
   - Vérifier que MongoDB passe à ✅ Ready dans la sidebar

4. **Partie 3** : Exécuter les requêtes MongoDB

5. **Partie 5** : Comparer les résultats (validation automatique)
   - Objectif : 100% de correspondance

6. **Partie 6** : Tester l'assistant IA avec vos propres questions

---

## ⚙️ Fonctionnalités Avancées

### Système de Cache Intelligent

```python
@st.cache_data(show_spinner=False)
def executer_toutes_les_requetes():
    # Calculs SQL cachés en mémoire par Streamlit
    ...
```

- **Cache Streamlit** : Les requêtes SQL et MongoDB ne sont exécutées qu'une seule fois
- **Cache disque** : Les résultats sont sauvegardés en CSV
- **Restauration automatique** : Au red\u00e9marrage, l'app recharge les CSV existants

### Gestion d'État (Session State)

```python
st.session_state["resultats_sql"]      # Résultats des requêtes SQL
st.session_state["resultats_mongo"]    # Résultats des requêtes MongoDB
st.session_state["migration_logs"]     # Journal de la dernière migration
st.session_state["mongo_uri"]          # URI MongoDB modifiable via l'interface
st.session_state["groq_api_key"]       # Clé API Groq modifiable via l'interface
```

### Sidebar "Admin"

- **État du Système** : Indicateurs visuels (SQLite/MongoDB)
- **État des Caches** : SQL/Mongo chargés ou vides
- **Config API & DB** : Modification dynamique de l'URI MongoDB et de la clé Groq
- **Danger Zone** 🧨 : Réinitialisation complète (suppression MongoDB + CSV + caches)

### Assistant IA - Mode JSON Strict

```python
response_format={"type": "json_object"}  # Force une réponse JSON valide
```

L'IA génère systématiquement un objet JSON structuré :

```json
{
  "collection": "lignes",
  "pipeline": [
    { "$match": { "type": "Bus" } },
    { "$group": { "_id": "$nom_ligne", "count": { "$sum": 1 } } }
  ],
  "explication": "Cette requête compte le nombre de bus par ligne"
}
```

---

## 📚 Structure du Projet

```
mongoDB_ST_QC_CA_JF/
│
├── app.py                          # Application principale (point d'entrée)
├── requirements.txt                # Dépendances Python
├── .env                            # Variables d'environnement (créé par l'utilisateur)
│
├── data/
│   ├── context_prompt.txt          # Prompt système pour l'IA (schéma MongoDB)
│   │
│   ├── sqlite/
│   │   ├── db/
│   │   │   └── paris2055.sqlite    # Base SQLite source (relationnelle)
│   │   │
│   │   ├── requetes_SQLite.py      # Fichier python des Requêtes SQL
│   │   └── resultats_requetes_sqlite/  # CSV des résultats SQL
│   │
│   │
│   └── mongodb/
│       ├── db/                     # Backup JSON des collections MongoDB
│       │   ├── Collection_capteurs.json         
│       │   ├── Collection_lignes.json
│       │   └── Collection_quartiers.json
│       │
│       ├── requetes_MongoDB.py      # Fichier python des Requêtes MongoDB
│       └── resultats_requetes_mongodb/  # CSV des résultats MongoDB
│
└── README.md                       # Ce fichier
```

---

## 📊 Statistiques

| Métrique | Valeur |
|----------|--------|
| Lignes de code | ~3 260 lignes (app.py) |
| Tables SQLite | 14 tables |
| Collections MongoDB | 3 collections |
| Requêtes métier | 14 requêtes (SQL + MongoDB) |
| Optimisations ETL | 4 caches pré-calculés |
| Modèle IA | Llama 3.3-70B (70 milliards de paramètres) |

---

## 🎓 Contexte Pédagogique

Ce projet a été développé dans le cadre du **BUT 3 Informatique - Parcours Sciences des Données** (2025) pour illustrer :

- ✅ La **migration de schéma** relationnel → NoSQL
- ✅ Les **optimisations ETL** (chunks, vectorisation Pandas, caches)
- ✅ Les **pipelines d'agrégation MongoDB** (équivalents SQL complexes)
- ✅ L'utilisation de **modèles d'IA génératifs** pour le requêtage en langage naturel
- ✅ La construction d'une **interface web interactive** avec Streamlit

---

## 📜 Licence

Projet pédagogique - BUT 3 Sciences des Données (2025)

---

## 🤝 Contributeurs

- Julien Forestier, Charles Auvrai, Quentin Chabot.

---

## 📞 Support

Pour toute question technique, consultez :
- **Code source** : app.py (commentaires détaillés)
- **Logs de migration** : Onglet "Partie 2 : Migration"
- **Documentation MongoDB** : [docs.mongodb.com](https://docs.mongodb.com/)
- **Documentation Groq** : [console.groq.com/docs](https://console.groq.com/docs)
