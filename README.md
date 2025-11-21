# 🚀 Projet : Migration de Données SQLite vers MongoDB (BUT 3)

Ce projet, réalisé dans le cadre du projet du BUT 3 Informatique (parcours SD), consiste à migrer un système d'information de transport urbain d'une base de données relationnelle (SQLite) vers une base de données NoSQL (MongoDB).

![Python](https://img.shields.io/badge/Python-3.10%2B-blue?style=for-the-badge&logo=python)
![Database](https://img.shields.io/badge/MongoDB-blue?style=for-the-badge&logo=mongodb)
![Database](https://img.shields.io/badge/SQLite-lightgrey?style=for-the-badge&logo=sqlite)

## 📖 Contexte : Paris 2055

Nous sommes en 2055. Paris a réinventé son réseau de transport après la tempête de 2050. L'objectif est de faire évoluer le système de suivi (lignes, arrêts, trafic, capteurs environnementaux) en migrant les données vers MongoDB pour mieux gérer les flux temps réel, les données semi-structurées et la visualisation géographique.

La base de données source est `Paris2055.sqlite`.

---

## 🏛️ Interface de Gestion Centrale (`main.py`)

Le cœur de ce projet est le script **`main.py`**.

Il sert d'interface utilisateur centrale (menu en console) pour piloter l'intégralité du projet. Il n'est pas nécessaire de lancer les scripts individuellement : `main.py` s'occupe de tout et vous guide à travers les différentes étapes du projet.

Cette interface centralise les 3 grandes parties du sujet (plus la visualisation) :

1.  **Partie 1 : Requêtes de Test (SQLite)**
    * Lance les 14 requêtes SQL sur la base `Paris2055.sqlite`.
    * Sauvegarde les résultats en fichiers `.csv` pour comparaison future.

2.  **Partie 2 : Script de Migration**
    * Exécute le processus ETL (Extract, Transform, Load).
    * Lit les données depuis SQLite, les restructure en documents JSON (en décidant du nombre de collections pertinent) et les insère dans la base MongoDB.

3.  **Partie 3 : Requêtes de Vérification (MongoDB)**
    * Exécute les requêtes équivalentes (via le *Aggregation Framework* de MongoDB) sur la nouvelle base NoSQL.
    * Permet de valider que la migration s'est déroulée sans perte de données.

4.  **(Partie 4 : Tableau de Bord)**
    * Lance le dashboard de visualisation (cartographie, carte choroplèthe) pour exploiter les données migrées.

---

## 🚀 Démarrage Rapide

Suivez ces étapes pour lancer le projet sur votre machine.

### 1. Prérequis

* Python 3.10 (ou supérieur)
* Une instance [MongoDB](https://www.mongodb.com/try/download/community) (locale ou via [Atlas](https://www.mongodb.com/cloud/atlas)) en cours d'exécution.
* Le fichier `Paris2055.sqlite` présent à la racine du projet.

### 2. Installation

1.  Clonez ce dépôt :
    ```bash
    git clone [https://github.com/qurnt1/BUT-3---Migration-SGBDR-vers-MongoDB-.git](https://github.com/qurnt1/BUT-3---Migration-SGBDR-vers-MongoDB-.git)
    cd BUT-3---Migration-SGBDR-vers-MongoDB-
    ```

2.  (Recommandé) Créez un environnement virtuel :
    ```bash
    python -m venv venv
    # Sur Windows
    .\venv\Scripts\Activate
    # Sur macOS/Linux
    source venv/bin/activate
    ```

3.  Installez les dépendances :
    Le fichier **`requirements.txt`** contient toutes les bibliothèques Python nécessaires (`pandas`, `pymongo`, `sqlite3`, etc.).

    ```bash
    pip install -r requirements.txt
    ```

### 3. Lancement

Assurez-vous que votre service MongoDB est accessible (vérifiez la chaîne de connexion dans les scripts si nécessaire).

Lancez l'interface principale et suivez les instructions :

```bash
python main.py