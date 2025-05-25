# 💳 Projet Data Engineering - Analyse Bancaire sur Microsoft Fabric

## 📌 Objectif

Ce projet met en œuvre un pipeline complet d’ingénierie de données dans le cloud autour d’un cas d’usage bancaire, en utilisant **Microsoft Fabric**.  
Il s’appuie sur le modèle en médaillon pour organiser les flux de données depuis l’ingestion brute jusqu’à la visualisation Power BI.

L’objectif est de démontrer la capacité à traiter, enrichir, historiser et exposer des données à grande échelle dans un environnement cloud moderne.

---

## 🧱 Architecture

Le projet suit l'**architecture en médaillon** :

- **Bronze** : Ingestion brute de fichiers CSV (clients, comptes, transactions)
- **Silver** : Nettoyage, enrichissement, typage, détection d’anomalies
- **Gold** : Modèle en étoile pour l’analyse Power BI
- **Power BI** : Tableau de bord connecté via Direct Lake

---

## 📁 Datasets utilisés

| Fichier            | Description                                  |
|--------------------|----------------------------------------------|
| `clients.csv`      | Informations personnelles des clients         |
| `accounts.csv`     | Soldes d'ouverture/clôture, nbre de transactions |
| `transactions.csv` | Montant des retraits, dépôts, solde courant   |

---

## 🔁 Pipeline technique

Les données sont traitées via 5 notebooks orchestrés dans un pipeline Fabric :

| Étape | Notebook                       | Description                                          |
|-------|--------------------------------|------------------------------------------------------|
| 1     | `01_ingestion_bronze.py`       | Ingestion brute des fichiers CSV                    |
| 2     | `02_transformation_silver.py`  | Nettoyage, typage, enrichissement                   |
| 3     | `03_modelisation_gold.py`      | Modèle dimensionnel (étoile)                        |
| 4     | `04_scd_et_versioning.py`      | Implémentation SCD1 (clients) et SCD2 (comptes)     |
| 5     | `05_powerbi_extraction.py`     | Génération de vues pour Power BI                    |

## 📊 Visualisation Power BI

Le tableau de bord Power BI contient :
- KPI : total des dépôts, retraits, nombre de clients
- Bar chart : activité mensuelle
- Carte : répartition par pays
- Pie chart : types de clients (entreprise / particulier)
- Tableau : détails client (solde moyen, nb comptes)

## ⚙️ Technologies
- **Microsoft Fabric** : Lakehouse, Notebooks, Pipelines
- **Power BI** : Tableau de bord dynamique
- **GitHub** : Versionnement et structuration du projet

---

## 📅 Auteurs

- **Auceane TITOT**
- **Lena DEMANOU**