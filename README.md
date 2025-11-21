#  Paris 2055 : Dashboard de Migration & Analytics (SQL  NoSQL)

Ce projet, réalisé dans le cadre du **BUT 3 Informatique  Parcours Sciences des Données**, est une solution complète de **Business Intelligence** et d'**ETL**. Il pilote la migration d'un systême de transport urbain futuriste d'une architecture **relationnelle (SQLite)** vers une architecture **orientée documents (MongoDB)**, le tout via une interface web interactive construite avec **Streamlit**.

![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.10%2B-blue?style=for-the-badge&logo=python&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-4EA94B?style=for-the-badge&logo=mongodb&logoColor=white)
![Groq AI](https://img.shields.io/badge/AI-Groq%20%2F%20Llama3-orange?style=for-the-badge)

---

##  Contexte : Paris 2055

En **2055**, le réseau de transport parisien génère des millions de données en temps réel :

- mesures de capteurs (**CO2**, **Bruit**, **Température**),
- informations de **trafic**, retards, incidents,
- données de **fréquentation** et de **quartiers**.

L'infrastructure historique basée sur **SQLite** atteint ses limites.

**Objectif pédagogique et technique :** concevoir un pipeline de migration vers **MongoDB** pour :

- supporter la volumétrie et l'évolution du modéle,
- dénormaliser intelligemment le schéma (modéle document),
- reproduire et enrichir les **indicateurs métiers**,
- ouvrir la voie à des **analyses assistées par IA** (Groq / Llama 3).

---

##  Fonctionnalités principales

L'application Streamlit est organisée en **5 onglets** :

###  Partie 1  Analyse SQL (Legacy)

- Exécution automatique de **14 requêtes métiers** (A  N) sur la base source **SQLite**.
- Calculs de KPIs : retards moyens, taux d'incidents, fréquentation, pollution, température, etc.
- **Mise en cache des résultats au format CSV** (`data/sqlite/resultats_requetes_sqlite/`) pour comparaison ultérieure.
- Affichage des résultats dans des **DataFrames interactifs**.

###  Partie 2  ETL & Migration vers MongoDB

Pipeline **ETL complet** :

#### Extract
- Lecture de l'ensemble des tables relationnelles : `Ligne`, `Quartier`, `Arret`, `Trafic`, `Capteur`, `Mesure`, `Vehicule`, `Chauffeur`, etc.

#### Transform
- Construction d'un **modêle document optimisé** :
  - `lignes` avec arréts imbriqués (`arrets`), horaires, véhicules et chauffeurs,
  - `capteurs` avec mesures imbriquées (`mesures`),
  - `quartiers` avec arréts associés et géométrie **GeoJSON** (`geom`).
- Dénormalisation raisonnée pour accélérer les requétes analytiques.

#### Load
- Insertion haute performance dans MongoDB via `insert_many` par **batchs**.
- Création d'**index** utiles (textuels et géospatiaux, `2dsphere`) pour les futures requétes.
- **Monitoring** : journal de migration en temps réel dans l'UI (logs détaillés, progression par pourcentage).
- Protection contre les plantages (try/except, messages d'erreur lisibles).

**Collections cibles créées dans la base MongoDB `Paris2055`:**

- `lignes`
- `quartiers`
- `capteurs`

###  Partie 3  Analytics NoSQL (MongoDB)

- Reproduction des **14 KPIs métiers** A  N, cette fois via le **MongoDB Aggregation Framework** sur les collections dénormalisées.
- Sauvegarde des résultats en CSV dans `data/mongodb/resultats_requetes_mongodb/`.
- Comparaison possible **SQL vs MongoDB** (performances, structure des résultats, cohérence).

###  Partie 4  Tableau de bord & Cartographie

- Espace dédié à la **visualisation avancée** (à enrichir) :
  - exploitation potentielle des champs **GeoJSON** (`geom`, `position`),
  - cartes de pollution, cartes de bruit,
  - dashboards de fréquentation par ligne/quartier.

> Cette partie est structurée dans le code et préte à accueillir des visuels (cartes, graphes, KPIs).

###  Partie 5  Assistant IA (Groq / Llama 3)

- Intégration d'un **assistant IA** via l'API **Groq** (modéle Llama 3).
- Traduction automatique du **langage naturel**  **pipeline d'agrégation MongoDB**.

**Fonctionnement :**

1. L'utilisateur saisit une question du type :
   >  Donne-moi les 5 lignes les plus polluées par le CO2 
   >  Classe les quartiers par niveau moyen de bruit 
   >  Moyenne des retards par type de ligne 

2. L'IA renvoie un objet JSON du type :
   ```json
   {
     "collection": "lignes",
     "pipeline": [
       { "$match": { /* ... */ } },
       { "$group": { /* ... */ } },
       { "$project": { "_id": 0, /* ... */ } }
     ]
   }
   ```

3. L'application :
   - affiche le pipeline généré,
   - l'exécute sur MongoDB,
   - et affiche le résultat sous forme de DataFrame.

---

##  Architecture technique

### Technologies

| Composant | Technologies |
|-----------|--------------|
| **Backend / ETL / Analytics** | Python, Pandas, SQLite, PyMongo |
| **Frontend** | Streamlit (UI multi-onglets) |
| **Base relationnelle** | SQLite (`data/sqlite/db/paris2055.sqlite`) |
| **Base NoSQL** | MongoDB (`Paris2055`, collections `lignes`, `quartiers`, `capteurs`) |
| **IA** | Groq API, modèle Llama 3 (assistant IA pour requétes MongoDB) |

### Organisation des dossiers

```
paris2055/
 app.py                          # Application principale Streamlit
 requirements.txt                # Dépendances Python
 README.md                       # Ce fichier
 data/
     sqlite/
        db/
           paris2055.sqlite    # Base relationnelle source
        resultats_requetes_sqlite/
            resultat_req_*.csv  # Résultats des requêtes SQL (cache)
     mongodb/
         collections/
            Collection_*.json   # Exports JSON intermédiaires (ETL)
         resultats_requetes_mongodb/
             resultat_req_*.csv  # Résultats des requêtes MongoDB (cache)
```

---

##  Installation et Lancement

### 1. Prérequis

- **Python 3.10** ou supérieur
- **MongoDB** :
  - instance locale (`mongodb://127.0.0.1:27017/`), ou
  - cluster MongoDB Atlas.
- **(Optionnel)** Une clé API Groq (pour la Partie 5) : à générer sur [https://console.groq.com](https://console.groq.com) (Et / ou demander a Quentin)

### 2. Cloner le projet et créer l'environnement
** Pour Windows
```bash
# Cloner le projet
git clone https://github.com/qurnt1/mongoDB_ST_QC_CA_JF.git

# Déplacer dans le dossier
cd mongoDB_ST_QC_CA_JF

# Création de l'environnement virtuel (recommandé)
python -m venv venv

# Activation de l'environnement virtuel
.\venv\Scripts\activate

# Création du fichier .env et écriture de la variable d'environnement (GROQ_API_KEY)
# Le fichier .env est créé à la racine du projet.
Set-Content -Path .env -Value 'GROQ_API_KEY=""'

# Installation des dépendances (dans l'environnement activé)
pip install -r requirements.txt
```

** Pour Mac / Linux

```bash
# Cloner le projet
git clone https://github.com/qurnt1/mongoDB_ST_QC_CA_JF.git

# Déplacer dans le dossier
cd mongoDB_ST_QC_CA_JF

# Création de l'environnement virtuel (recommandé)
python -m venv venv

# Activation de l'environnement virtuel
source venv/bin/activate

# Création du fichier .env et écriture de la variable d'environnement (GROQ_API_KEY)
# Le fichier .env est créé à la racine du projet.
echo 'GROQ_API_KEY=""' > .env

# Installation des dépendances (dans l'environnement activé)
pip install -r requirements.txt
```

### 3. Configuration

#### Base SQLite

La base SQLite doit étre disponible é l'emplacement suivant (créé automatiquement si besoin) :

```
data/sqlite/db/paris2055.sqlite
```

Si le fichier porte un autre nom ou se trouve ailleurs, adaptez la constante :

```python
DB_FILE = os.path.join("data", "sqlite", "db", "paris2055.sqlite")
```

#### Connexion MongoDB

Par défaut, l'application utilise :

```python
MONGO_URI = "mongodb://127.0.0.1:27017/"
MONGO_DB_NAME = "Paris2055"
```

Pour une autre instance ou un cluster Atlas, adaptez la valeur de `MONGO_URI` (et éventuellement `MONGO_DB_NAME`) dans le fichier principal.

#### Clé API Groq (Assistant IA)

Dans le code, la clé est définie par :

```python
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
```

Si vous n'avez pas la clé API, demandez la a Quentin et mettez la dans votre fichier .venv (ou .env) 
sous le format suivant :
GROQ_API_KEY = "gsk_XXXXXXXXXXX"


**Variante (si vous préférez ne pas utiliser les variables d'environnements):** vous pouvez adapter le code pour lire `GROQ_API_KEY = "{votre clef api}"` au lieu de fixer la clé en variable d'environement.

### 4. Lancement de l'application

Depuis la racine du projet (environnement virtuel activé) :

```bash
streamlit run app.py
```

Un onglet s'ouvre dans votre navigateur (par défaut sur `http://localhost:8501`).

---

##  Parcours utilisateur recommandé

### Partie 1  SQL

1. Vérifier le bon fonctionnement de la base SQLite.
2. Cliquer sur  Executer Requetes  pour générer les 14 KPIs.
3. Les résultats sont également exportés en CSV pour audit/comparaison.

### Partie 2  Migration

1. Cliquer sur  Lancer Migration .
2. Suivre l'évolution du journal de migration (logs détaillés).
3. é la fin, les collections `lignes`, `quartiers`, `capteurs` sont créées dans MongoDB, avec leurs index.

### Partie 3  MongoDB

1. Vérifier que le serveur MongoDB est connecté et que la base `Paris2055` existe (panneau latéral).
2. Cliquer sur  Executer Requetes MongoDB  pour recalculer les 14 KPIs sur le modéle NoSQL.
3. Comparer les résultats SQL vs MongoDB pour valider la migration.

### Partie 4  Dashboard

- Utiliser cet onglet pour développer des visualisations avancées (cartes, graphiques, heatmaps, etc.) é partir des collections MongoDB.

### Partie 5  Assistant IA

1. Saisir une question en langage naturel.
2. Laisser l'IA générer le pipeline d'agrégation, puis l'exécuter.
3. Inspecter le JSON proposé et le résultat agrégé associé.

---

##  Aspects techniques notables

### Migration progressive & robuste

- Groupements par clé métier (`id_ligne`, `id_arret`, `id_capteur`, etc.).
- Conversion sécurisée des dates (`to_datetime`) et des coordonnées (GeoJSON).
- Construction de sous-documents imbriqués (`arrets`, `horaire`, `vehicule`, `chauffeur`, `mesures`, `incidents`).

### Performance

- Traitement par batchs (ex. 25 000 documents) pour limiter la consommation mémoire et les tailles de requétes Mongo.
- Logs de progression contrélés (`step_percent`) pour éviter de saturer l'interface Streamlit.

### Cohérence des résultats

- Requétes A  N réécrites é l'identique :
  - d'abord en SQL (sur la base normalisée),
  - puis en MongoDB Aggregation (sur le modéle document).
- Les CSV générés cété SQL / Mongo facilitent l'audit et les comparaisons.
  
---

##  Licence

Ce projet est réalisé dans un cadre pédagogique.

```
Copyright (c) 2025 

Utilisation autorisée à des fins pédagogiques et de démonstration.
Toute réutilisation ou diffusion extérieure doit étre validée par les auteurs.
```
