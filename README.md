
# Paris 2055 : Dashboard de Migration & Analytics (SQL ➡️ NoSQL)

Ce projet, réalisé dans le cadre du **BUT 3 Informatique – Parcours Sciences des Données**, est une solution complète de **Business Intelligence** et d'**ETL**. Il pilote la migration d'un système de transport urbain futuriste d'une architecture **relationnelle (SQLite)** vers une architecture **orientée documents (MongoDB)**, le tout via une interface web interactive construite avec **Streamlit**.

![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.10%2B-blue?style=for-the-badge&logo=python&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-4EA94B?style=for-the-badge&logo=mongodb&logoColor=white)
![Groq AI](https://img.shields.io/badge/AI-Groq%20%2F%20Llama3.3-orange?style=for-the-badge)

---

## 🏙️ Contexte : Paris 2055

En **2055**, le réseau de transport parisien génère des millions de données en temps réel :
- mesures de capteurs (**CO2**, **Bruit**, **Température**),
- informations de **trafic**, retards, incidents,
- données de **fréquentation** et de **quartiers**.

L'infrastructure historique basée sur **SQLite** atteint ses limites.

**Objectif technique :** Concevoir un pipeline de migration vers **MongoDB** pour supporter la volumétrie, dénormaliser le schéma (modèle document) et permettre des analyses assistées par **IA Générative**.

---

## 🚀 Fonctionnalités principales

L'application Streamlit est organisée en **6 onglets** :

| Partie | Nom | Description |
|--------|-----|-------------|
| 1️⃣ | Analyse SQL (Legacy) | 14 requêtes métiers sur SQLite, cache CSV |
| 2️⃣ | ETL & Migration | Extract, Transform (caches `vehicules_cache`, `stats_trafic`), Load vers MongoDB |
| 3️⃣ | Analytics NoSQL | KPIs via MongoDB Aggregation Framework |
| 4️⃣ | Tableau de bord | Visualisations et cartes interactives |
| 5️⃣ | Validation & Comparaison | Comparaison automatisée SQL vs MongoDB |
| 6️⃣ | Assistant IA | Llama 3.3 avec mode JSON strict |

---

## 🛠️ Architecture & Optimisations

### Optimisations de Migration
- **Caches imbriqués** : `vehicules_cache`, `chauffeurs_cache`, `stats_trafic`
- **Géométrie GeoJSON** : Quartiers et capteurs
- **Indexation MongoDB** : `2dsphere`, indexation composite

### Sidebar "Admin"
- Vérification état SQLite / MongoDB
- Gestion clé API Groq
- **Danger Zone** : Reset base et caches

---

## 📦 Installation et Lancement

### Prérequis
- Python 3.10+
- MongoDB (Local ou Atlas)
- Clé API Groq

### Installation

```bash
git clone https://github.com/qurnt1/mongoDB_ST_QC_CA_JF.git
cd mongoDB_ST_QC_CA_JF
python -m venv venv
# Windows: .\venv\Scripts\activate
# Mac/Linux: source venv/bin/activate
pip install -r requirements.txt
```

### Configuration (.env)
```env
GROQ_API_KEY="gsk_xxxxxxxxxxxxxxxxxxxxxxxx"
```

### Lancement
```bash
streamlit run app.py
```

---

## 🚦 Parcours utilisateur

1. **Partie 1** : Générez données SQL
2. **Partie 2** : Lancez migration
3. **Partie 3** : Validez requêtes NoSQL
4. **Partie 5** : Comparez résultats
5. **Partie 6** : Interrogez l'IA

---

## 📜 Licence

Projet pédagogique - BUT 3 Informatique (2025)
