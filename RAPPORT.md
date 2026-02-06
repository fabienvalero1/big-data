# Rapport de Projet Big Data : Buy & Rent

## 1. Problématique Métier

Dans un marché immobilier dynamique, les investisseurs ont besoin d'identifier rapidement les opportunités rentables. Ce projet vise à construire une plateforme capable de :

- **Collecter** des données hétérogènes (annonces, risques, taux)
- **Ingérer** ces données en streaming via Apache Kafka
- **Transformer** ces données avec Apache Spark pour calculer des indicateurs clés
- **Stocker** les résultats dans un modèle analytique
- **Visualiser** les tendances via des dashboards

## 2. Architecture Globale

Nous utilisons une architecture **Lambda simplifiée** combinant batch et streaming :

| Composant | Technologie | Rôle |
|-----------|-------------|------|
| Orchestration | Apache Airflow | Planification et exécution des pipelines |
| Ingestion Streaming | Apache Kafka | File de messages pour l'ingestion temps réel |
| Traitement Batch | Apache Spark | Calculs distribués et transformations |
| Base de données | PostgreSQL | Stockage des données (Data Warehouse) |
| Logs | Loki + Promtail | Collecte et agrégation des logs |
| Visualisation | Grafana | Dashboards et monitoring |

### Avantages de cette architecture :
- **Reproductibilité** : Les DAGs Airflow définissent clairement le pipeline
- **Scalabilité** : Kafka et Spark permettent de traiter de gros volumes
- **Découplage** : Kafka sépare la collecte du traitement
- **Performance** : Spark traite les données en parallèle sur le cluster
- **Observabilité** : Loki/Grafana permettent de monitorer l'exécution

## 3. Apache Kafka - Ingestion Streaming

### 3.1 Rôle de Kafka dans l'architecture

Apache Kafka sert de **couche d'ingestion** entre les collecteurs de données et le traitement Spark. Il permet de :

- **Découpler** les producteurs (collecteurs) des consommateurs (Spark)
- **Bufferiser** les données en cas de pic de charge
- **Garantir** la persistance des messages
- **Permettre** le rejeu des données si nécessaire

### 3.2 Topics Kafka

| Topic | Description | Partitions |
|-------|-------------|------------|
| `raw-listings` | Annonces immobilières brutes | 3 |
| `georisks` | Risques géographiques par commune | 1 |
| `rates` | Taux financiers actuels | 1 |

### 3.3 Flux de données Kafka

```
┌──────────────────┐     ┌─────────────────┐     ┌──────────────────┐
│ Listings Collector│────▶│  Topic          │────▶│                  │
│                  │     │  raw-listings   │     │                  │
└──────────────────┘     └─────────────────┘     │                  │
                                                  │   Kafka          │
┌──────────────────┐     ┌─────────────────┐     │   Consumer       │
│ Georisks Collector│───▶│  Topic          │────▶│        ▼         │
│                  │     │  georisks       │     │   Fichiers JSON  │
└──────────────────┘     └─────────────────┘     │   pour Spark     │
                                                  │                  │
┌──────────────────┐     ┌─────────────────┐     │                  │
│ Rates Collector  │────▶│  Topic          │────▶│                  │
│                  │     │  rates          │     └──────────────────┘
└──────────────────┘     └─────────────────┘
```

## 4. Apache Spark - Traitement Distribué

### 4.1 Rôle de Spark dans l'architecture

Apache Spark est utilisé comme **moteur de traitement batch** pour :

- **Joindre** les annonces avec les données de risques
- **Calculer** les indicateurs financiers (rentabilité, cashflow, score)
- **Transformer** les données brutes en données exploitables
- **Paralléliser** les calculs sur le cluster

### 4.2 Architecture Spark

```
┌─────────────────────────────────────────────────────┐
│                   SPARK CLUSTER                      │
│                                                      │
│  ┌──────────────┐         ┌──────────────┐          │
│  │ Spark Master │◄───────▶│ Spark Worker │          │
│  │  (port 7077) │         │              │          │
│  └──────────────┘         └──────────────┘          │
│         │                                            │
│         ▼                                            │
│  ┌──────────────────────────────────────┐           │
│  │        transform_listings.py          │           │
│  │  - Lecture JSON (listings, risques)   │           │
│  │  - Jointure sur code_insee            │           │
│  │  - Calcul mensualité                  │           │
│  │  - Calcul loyer estimé                │           │
│  │  - Calcul cashflow                    │           │
│  │  - Calcul rentabilité                 │           │
│  │  - Score d'investissement             │           │
│  │  - Export JSON enrichi                │           │
│  └──────────────────────────────────────┘           │
└─────────────────────────────────────────────────────┘
```

### 4.3 Job Spark : transform_listings.py

Le job Spark effectue les transformations suivantes :

| Transformation | Description | Formule |
|----------------|-------------|---------|
| Mensualité | Calcul du remboursement mensuel | `P × r(1+r)^n / ((1+r)^n - 1)` |
| Loyer estimé | Estimation basée sur 5% de rendement brut | `prix × 0.05 / 12` |
| Cashflow | Flux de trésorerie mensuel | `loyer - mensualité - 150€` |
| Rentabilité brute | Rendement annuel en % | `(loyer × 12 / prix) × 100` |
| Score investissement | Note de 0 à 10 | Basé sur cashflow et rentabilité |

## 5. Pipeline de Données Complet

Le DAG `buy_and_rent_pipeline` s'exécute toutes les heures et comprend **9 tâches** :

```
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│collect_listings │   │collect_georisks │   │  collect_rates  │
└────────┬────────┘   └────────┬────────┘   └────────┬────────┘
         │                     │                     │
         ▼                     ▼                     ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│ send_to_kafka   │   │ send_to_kafka   │   │ send_to_kafka   │
│  (raw-listings) │   │   (georisks)    │   │    (rates)      │
└────────┬────────┘   └────────┬────────┘   └────────┬────────┘
         │                     │                     │
         └──────────┬──────────┴──────────┬──────────┘
                    │                     │
                    ▼                     ▼
           ┌─────────────────┐   ┌─────────────────┐
           │prepare_spark_data│◄──│   (collectes)   │
           └────────┬────────┘   └─────────────────┘
                    │
                    ▼
           ┌─────────────────┐
           │  spark_transform │  ← Job Spark (enrichissement)
           └────────┬────────┘
                    │
                    ▼
           ┌─────────────────┐
           │load_spark_results│
           └────────┬────────┘
                    │
                    ▼
           ┌─────────────────┐
           │ load_to_postgres │
           └─────────────────┘
```

### Détail des tâches :

| Phase | Tâche | Description |
|-------|-------|-------------|
| 1. Collecte | `collect_listings` | Génère des annonces immobilières (simulation Faker) |
| 1. Collecte | `collect_georisks` | Récupère les risques naturels (API Géorisques) |
| 1. Collecte | `collect_rates` | Récupère les taux d'intérêt (scraping Euribor) |
| 2. Ingestion | `send_listings_to_kafka` | Envoie les annonces vers Kafka |
| 2. Ingestion | `send_georisks_to_kafka` | Envoie les risques vers Kafka |
| 2. Ingestion | `send_rates_to_kafka` | Envoie les taux vers Kafka |
| 3. Préparation | `prepare_spark_data` | Prépare les fichiers JSON pour Spark |
| 4. Traitement | `spark_transform` | Exécute le job Spark de transformation |
| 5. Chargement | `load_spark_results` | Charge les résultats Spark |
| 6. Stockage | `load_to_postgres` | Insère les données dans PostgreSQL |

## 6. Modèle de Données (Star Schema)

| Table | Type | Description |
|-------|------|-------------|
| `fact_listings` | Fait | Annonces enrichies avec indicateurs financiers |
| `dim_location` | Dimension | Informations géographiques (villes, régions) |
| `ref_georisques` | Référence | Risques naturels par commune |
| `ref_taux` | Référence | Taux financiers historiques |

## 7. Stack Technique Docker

| Service | Image | Port | Rôle |
|---------|-------|------|------|
| PostgreSQL | postgres:15 | 5433 | Data Warehouse |
| Airflow Webserver | apache/airflow | 8080 | Interface DAG |
| Airflow Scheduler | apache/airflow | - | Planification |
| Kafka | confluentinc/cp-kafka:7.5.0 | 9092 | Message Broker |
| Spark Master | apache/spark:4.0.1-python3 | 7077, 8081 | Coordination Spark |
| Spark Worker | apache/spark:4.0.1-python3 | - | Exécution Spark |
| Loki | grafana/loki | 3100 | Agrégation logs |
| Promtail | grafana/promtail | - | Collecte logs |
| Grafana | grafana/grafana | 3000 | Dashboards |

## 8. Résultats et Conclusions

La plateforme permet :
- **Ingestion streaming** via Kafka pour découpler collecte et traitement
- **Traitement distribué** via Spark pour des calculs parallélisés
- **Calcul d'indicateurs** d'investissement en temps quasi-réel
- **Stockage structuré** pour analyse BI (star schema)
- **Monitoring complet** via Grafana

### Points forts de l'architecture :
- **Scalabilité horizontale** : Kafka et Spark peuvent scaler sur plusieurs nœuds
- **Tolérance aux pannes** : Kafka persiste les messages, Spark peut rejouer les jobs
- **Modularité** : Chaque composant est indépendant et remplaçable
- **Observabilité** : Logs centralisés et métriques disponibles

### Évolutions possibles :
- Intégration d'APIs réelles (SeLoger, LeBonCoin)
- Spark Streaming pour du traitement temps réel continu
- Ajout de modèles ML pour prédiction de prix
- Alertes automatiques sur opportunités
- Delta Lake pour la gestion des versions de données

## 9. Auteurs

- Gael Tuczapski
- Fabien Valero
- Emmanuel Lion
