# Architecture Big Data - Projet Buy & Rent

Ce projet implémente une plateforme Big Data pour l'analyse d'opportunités d'investissement immobilier.

## Architecture

L'architecture utilise **Apache Airflow** pour l'orchestration des pipelines de données :

```
┌─────────────────────────────────────────────────────────────────┐
│                        AIRFLOW (Orchestration)                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
│  │   Collect    │  │   Collect    │  │   Collect    │           │
│  │  Listings    │  │  Georisks    │  │    Rates     │           │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘           │
│         │                 │                 │                    │
│         └─────────────────┼─────────────────┘                    │
│                           ▼                                      │
│                   ┌──────────────┐                               │
│                   │   Enrich &   │                               │
│                   │  Transform   │                               │
│                   └──────┬───────┘                               │
│                          │                                       │
│                          ▼                                       │
│                   ┌──────────────┐                               │
│                   │    Load to   │                               │
│                   │  PostgreSQL  │                               │
│                   └──────────────┘                               │
└─────────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                     POSTGRESQL (Data Warehouse)                  │
│  fact_listings │ dim_location │ ref_georisques │ ref_taux       │
└─────────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  MONITORING: Loki + Promtail (Logs) │ Grafana (Dashboards)      │
└─────────────────────────────────────────────────────────────────┘
```

## Comment lancer le projet

### Pré-requis
- Docker Desktop installé et démarré

### Lancement

1. **Initialiser Airflow** (première fois uniquement) :
   ```bash
   docker-compose up airflow-init
   ```

2. **Démarrer tous les services** :
   ```bash
   docker-compose up -d
   ```

3. **Accéder aux interfaces** :
   - **Airflow** : http://localhost:8080 (user: `airflow`, password: `airflow`)
   - **Grafana** : http://localhost:3000 (user: `admin`, password: `admin`)
   - **PostgreSQL** : `localhost:5433` (user: `airflow`, password: `airflow`)

4. **Déclencher le pipeline** :
   - Allez dans Airflow → DAGs → `buy_and_rent_pipeline`
   - Cliquez sur "Trigger DAG"

5. **Vérifier les données** :
   ```bash
   docker exec -it $(docker ps -qf "name=postgres") psql -U airflow -d airflow -c "SELECT * FROM fact_listings LIMIT 5;"
   ```

## Structure du projet

```
.
├── airflow/
│   ├── dags/                # DAGs Airflow
│   ├── logs/                # Logs Airflow
│   └── plugins/             # Plugins Airflow
├── src/
│   ├── data_collectors/     # Collecteurs de données
│   ├── transformers/        # Transformations
│   └── loaders/             # Chargement en base
├── sql/
│   └── init.sql             # Schéma de base de données
├── monitoring/
│   ├── promtail/            # Config Promtail
│   └── grafana/             # Données Grafana
├── docker-compose.yml       # Orchestration Docker
├── .env                     # Variables d'environnement
└── README.md
```

## Pipeline de données

1. **Collecte** (en parallèle) :
   - Annonces immobilières (simulées via Faker)
   - Risques géographiques (API Géorisques simulée)
   - Taux financiers (Banque de France simulée)

2. **Transformation** :
   - Enrichissement avec les risques
   - Calcul de la rentabilité brute
   - Calcul du cashflow mensuel
   - Score d'investissement

3. **Chargement** :
   - Insertion dans PostgreSQL (modèle en étoile)

## Auteurs
- Gael Tuczapski
- Fabien Valero
- Emmanuel Lion

