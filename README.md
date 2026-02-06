# Architecture Big Data - Projet Buy & Rent

Ce projet implémente une plateforme Big Data pour l'analyse d'opportunités d'investissement immobilier, utilisant **Apache Kafka** pour l'ingestion streaming et **Apache Spark** pour le traitement distribué.

## Architecture

L'architecture utilise une approche **Lambda simplifiée** orchestrée par **Apache Airflow** :

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          AIRFLOW (Orchestration)                             │
│                                                                              │
│   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐                    │
│   │   Collect    │   │   Collect    │   │   Collect    │    PHASE 1:        │
│   │  Listings    │   │  Georisks    │   │    Rates     │    COLLECTE        │
│   └──────┬───────┘   └──────┬───────┘   └──────┬───────┘                    │
│          │                  │                  │                             │
│          ▼                  ▼                  ▼                             │
│   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐                    │
│   │  Send to     │   │  Send to     │   │  Send to     │    PHASE 2:        │
│   │  Kafka       │   │  Kafka       │   │  Kafka       │    INGESTION       │
│   └──────┬───────┘   └──────┬───────┘   └──────┬───────┘                    │
│          │                  │                  │                             │
└──────────┼──────────────────┼──────────────────┼────────────────────────────┘
           │                  │                  │
           ▼                  ▼                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        KAFKA (Message Broker)                                │
│   ┌────────────────┐  ┌────────────────┐  ┌────────────────┐                │
│   │  raw-listings  │  │   georisks     │  │    rates       │                │
│   │    (topic)     │  │    (topic)     │  │    (topic)     │                │
│   └────────────────┘  └────────────────┘  └────────────────┘                │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SPARK CLUSTER                                        │
│                                                                              │
│   ┌──────────────┐              ┌──────────────┐                            │
│   │ Spark Master │◄────────────▶│ Spark Worker │       PHASE 3:             │
│   │  (port 7077) │              │              │       TRAITEMENT           │
│   └──────────────┘              └──────────────┘                            │
│          │                                                                   │
│          ▼                                                                   │
│   ┌────────────────────────────────────────────┐                            │
│   │         transform_listings.py               │                            │
│   │  • Jointure annonces + risques              │                            │
│   │  • Calcul rentabilité, cashflow, score      │                            │
│   │  • Export données enrichies                 │                            │
│   └────────────────────────────────────────────┘                            │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     POSTGRESQL (Data Warehouse)                              │
│   fact_listings │ dim_location │ ref_georisques │ ref_taux                  │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│   MONITORING: Loki + Promtail (Logs) │ Prometheus │ Grafana (Dashboards)    │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Stack Technique

| Service | Technologie | Port | Description |
|---------|-------------|------|-------------|
| Orchestration | Apache Airflow | 8080 | Planification et exécution des DAGs |
| Message Broker | Apache Kafka | 9092 | Ingestion streaming des données |
| Traitement | Apache Spark | 7077, 8081 | Calculs distribués |
| Data Warehouse | PostgreSQL | 5433 | Stockage analytique |
| Logs | Loki + Promtail | 3100 | Agrégation des logs |
| Dashboards | Grafana | 3000 | Visualisation |

## Comment lancer le projet

### Pré-requis
- Docker Desktop installé et démarré
- Au moins 8 Go de RAM disponible

### Lancement

1. **Initialiser Airflow** (première fois uniquement) :
   ```bash
   docker-compose up airflow-init
   ```

2. **Démarrer tous les services** :
   ```bash
   docker-compose up -d
   ```

3. **Vérifier que tous les services sont démarrés** :
   ```bash
   docker-compose ps
   ```

4. **Accéder aux interfaces** :
   - **Airflow** : http://localhost:8080 (user: `airflow`, password: `airflow`)
   - **Spark Master** : http://localhost:8081
   - **Grafana** : http://localhost:3000 (user: `admin`, password: `admin`)
   - **PostgreSQL** : `localhost:5433` (user: `airflow`, password: `airflow`)

5. **Déclencher le pipeline** :
   - Allez dans Airflow → DAGs → `buy_and_rent_pipeline`
   - Cliquez sur "Trigger DAG"

6. **Vérifier les données** :
   ```bash
   docker exec -it $(docker ps -qf "name=postgres") psql -U airflow -d airflow -c "SELECT * FROM fact_listings LIMIT 5;"
   ```

### Commandes utiles Kafka

```bash
# Accéder au conteneur Kafka
docker exec -it kafka bash

# Lister les topics
kafka-topics --list --bootstrap-server localhost:9092

# Consommer les messages d'un topic
kafka-console-consumer --bootstrap-server localhost:9092 --topic raw-listings --from-beginning
```

### Commandes utiles Spark

```bash
# Accéder au conteneur Spark Master
docker exec -it big-data-spark-master-1 bash

# Lancer PySpark interactif
/opt/spark/bin/pyspark

# Soumettre un job Spark
/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/transform_listings.py
```

## Structure du projet

```
.
├── airflow/
│   ├── dags/                    # DAGs Airflow
│   │   └── buy_and_rent_dag.py  # Pipeline principal (9 tâches)
│   ├── logs/                    # Logs Airflow
│   └── plugins/                 # Plugins Airflow
├── src/
│   ├── data_collectors/         # Collecteurs de données
│   │   ├── listings_collector.py
│   │   ├── georisks_collector.py
│   │   └── rates_collector.py
│   ├── kafka/                   # Modules Kafka
│   │   ├── producer.py          # Producteur Kafka
│   │   └── consumer.py          # Consommateur Kafka
│   ├── transformers/            # Transformations Python (fallback)
│   │   └── enrichment.py
│   └── loaders/                 # Chargement en base
│       └── postgres_loader.py
├── spark_jobs/                  # Jobs Spark
│   └── transform_listings.py    # Job d'enrichissement Spark
├── sql/
│   └── init.sql                 # Schéma de base de données
├── scripts/
│   └── init_kafka_topics.sh     # Initialisation des topics Kafka
├── data/                        # Données partagées Airflow/Spark
├── monitoring/
│   ├── promtail/                # Config Promtail
│   └── prometheus.yml/          # Config Prometheus
├── docker-compose.yml           # Orchestration Docker
├── .env                         # Variables d'environnement
├── README.md                    # Ce fichier
└── RAPPORT.md                   # Rapport détaillé du projet
```

## Pipeline de données

Le DAG `buy_and_rent_pipeline` comprend **9 tâches** organisées en 6 phases :

### Phase 1 : Collecte (en parallèle)
- **collect_listings** : Génère des annonces immobilières (Faker)
- **collect_georisks** : Récupère les risques via API Géorisques
- **collect_rates** : Scrape les taux Euribor

### Phase 2 : Ingestion Kafka
- **send_listings_to_kafka** : Envoie vers topic `raw-listings`
- **send_georisks_to_kafka** : Envoie vers topic `georisks`
- **send_rates_to_kafka** : Envoie vers topic `rates`

### Phase 3 : Préparation Spark
- **prepare_spark_data** : Prépare les fichiers JSON pour Spark

### Phase 4 : Traitement Spark
- **spark_transform** : Exécute le job Spark (jointures, calculs financiers)

### Phase 5-6 : Chargement
- **load_spark_results** : Charge les résultats Spark
- **load_to_postgres** : Insère dans PostgreSQL

## Indicateurs calculés par Spark

| Indicateur | Description | Formule |
|------------|-------------|---------|
| Mensualité | Remboursement mensuel du prêt | `P × r(1+r)^n / ((1+r)^n - 1)` |
| Loyer estimé | Estimation du loyer mensuel | `prix × 5% / 12` |
| Cashflow | Flux de trésorerie mensuel | `loyer - mensualité - 150€` |
| Rentabilité brute | Rendement annuel | `(loyer × 12 / prix) × 100` |
| Score | Note d'investissement (0-10) | Basé sur cashflow et rentabilité |

## Auteurs

- Gael Tuczapski
- Fabien Valero
- Emmanuel Lion
