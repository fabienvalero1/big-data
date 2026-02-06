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
- Git installé

### Lancement

1. **Cloner le projet** :
   ```bash
   git clone <url-du-repo>
   cd big-data
   ```

2. **Configurer l'environnement** :
   ```bash
   # Copier le fichier d'exemple (si nécessaire)
   cp .env.example .env
   ```

3. **Initialiser Airflow** (première fois uniquement) :
   ```bash
   docker-compose up airflow-init
   ```

4. **Démarrer tous les services** :
   ```bash
   docker-compose up -d
   ```

5. **Vérifier que tous les services sont démarrés** :
   ```bash
   docker-compose ps
   ```

   Attendez que tous les services soient en status "healthy" ou "Up".

6. **Accéder aux interfaces** :

   | Service | URL | Identifiants |
   |---------|-----|--------------|
   | **Airflow** | http://localhost:8082 | airflow / airflow |
   | **Spark Master** | http://localhost:8081 | - |
   | **Grafana** | http://localhost:3000 | admin / admin |
   | **PostgreSQL** | localhost:5433 | airflow / airflow |

7. **Déclencher le pipeline** :
   - Allez dans Airflow → DAGs → `buy_and_rent_pipeline`
   - Activez le DAG (toggle ON)
   - Cliquez sur "Play" (▶) → "Trigger DAG"

8. **Vérifier les données** :
   ```bash
   # Linux/Mac
   docker exec -it big-data-postgres-1 psql -U airflow -d airflow -c "SELECT COUNT(*) FROM fact_listings;"

   # Windows PowerShell
   docker exec -it big-data-postgres-1 psql -U airflow -d airflow -c "SELECT COUNT(*) FROM fact_listings;"
   ```

### Arrêter le projet

```bash
# Arrêter tous les services
docker-compose down

# Arrêter et supprimer les volumes (reset complet)
docker-compose down -v
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

## Troubleshooting

### Port 8080 déjà utilisé
Si le port 8080 est déjà utilisé (ex: autre service), Airflow utilise le port **8082** par défaut dans ce projet.

### Erreur "Invalid username or password" sur Airflow
```bash
# Recréer l'utilisateur admin
docker-compose run airflow-webserver airflow users create \
  --username airflow --password airflow \
  --firstname Admin --lastname User \
  --role Admin --email admin@example.com
```

### Erreur Spark "Mkdirs failed to create"
C'est un problème de permissions. Les conteneurs Spark sont configurés avec `user: root` pour éviter ce problème. Si l'erreur persiste :
```bash
# Supprimer les anciens dossiers batch
# Linux/Mac
rm -rf data/batch_*

# Windows PowerShell
Remove-Item -Recurse -Force data\batch_*

# Redémarrer les conteneurs
docker-compose down
docker-compose up -d
```

### Erreur "can't adapt type 'Proxy'" dans PostgreSQL
Cette erreur est liée aux types Airflow XCom. Le code a été corrigé pour convertir automatiquement les Proxy en types Python natifs via JSON serialization.

### Les données ne s'insèrent pas dans PostgreSQL
Vérifiez les logs de la tâche `load_to_postgres` dans Airflow. Les erreurs courantes :
- Proxy types (corrigé)
- Connexion à la base (vérifier que postgres est healthy)

### Grafana n'affiche pas de données
1. Vérifiez que PostgreSQL contient des données :
   ```bash
   docker exec -it big-data-postgres-1 psql -U airflow -d airflow -c "SELECT COUNT(*) FROM fact_listings;"
   ```
2. Dans Grafana, ajustez la plage de temps (Time range) sur "Last 24 hours" ou "Today"
3. Vérifiez que le datasource PostgreSQL est bien configuré

### Recharger le code Python après modification
Après avoir modifié un fichier Python dans `src/`, redémarrez Airflow :
```bash
docker-compose restart airflow-scheduler airflow-webserver
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
│   │   ├── listings_collector.py   # Génère 100 annonces/batch
│   │   ├── georisks_collector.py
│   │   └── rates_collector.py
│   ├── kafka_utils/             # Modules Kafka (renommé pour éviter conflit)
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
│   └── batch_*/                 # Dossiers de batch (générés automatiquement)
├── monitoring/
│   ├── grafana/
│   │   └── provisioning/        # Configuration Grafana automatique
│   │       ├── datasources/     # Datasources (PostgreSQL, Loki)
│   │       └── dashboards/      # Dashboards JSON
│   ├── promtail/                # Config Promtail
│   └── prometheus.yml           # Config Prometheus
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

## Configuration Grafana

### Datasources préconfigurés

Les datasources sont automatiquement chargés au démarrage via le provisioning :

- **PostgreSQL** : Connexion au Data Warehouse (`postgres:5432`)
- **Loki** : Agrégation des logs (`loki:3100`)

### Requêtes SQL utiles pour Grafana

```sql
-- Évolution du nombre d'annonces par heure
SELECT
  date_trunc('hour', date_creation) as time,
  COUNT(*) as "Nb annonces"
FROM fact_listings
GROUP BY 1 ORDER BY 1

-- Distribution par ville
SELECT
  CASE code_insee
    WHEN '75056' THEN 'Paris'
    WHEN '69123' THEN 'Lyon'
    WHEN '13055' THEN 'Marseille'
    WHEN '33063' THEN 'Bordeaux'
    WHEN '42218' THEN 'Saint-Étienne'
    ELSE code_insee
  END as "Ville",
  COUNT(*) as "Nb annonces",
  AVG(prix_acquisition) as "Prix moyen"
FROM fact_listings
GROUP BY code_insee

-- Évolution des indicateurs financiers
SELECT
  date_creation as time,
  AVG(cashflow) as "Cashflow moyen",
  AVG(rentabilite_brute) as "Rentabilité moyenne",
  AVG(score_investissement) as "Score moyen"
FROM fact_listings
GROUP BY date_creation
ORDER BY date_creation

-- Top 10 meilleures opportunités
SELECT
  listing_id,
  prix_acquisition as "Prix",
  loyer_estime as "Loyer",
  cashflow as "Cashflow",
  score_investissement as "Score"
FROM fact_listings
ORDER BY score_investissement DESC
LIMIT 10
```

### Exporter/Importer un dashboard

Pour sauvegarder un dashboard et le versionner :

1. Dans Grafana, ouvrez votre dashboard
2. Cliquez sur ⚙️ Settings → JSON Model
3. Copiez le JSON dans `monitoring/grafana/provisioning/dashboards/mon-dashboard.json`
4. Redémarrez Grafana : `docker-compose restart grafana`

## Auteurs

- Gaël TUCZAPSKI
- Fabien VALERO
- Emmanuel LION
