# Architecture Big Data et Streaming - Projet Buy & Rent

Ce projet est une refonte de l'application "Buy & Rent" en une architecture **Big Data & Streaming** utilisant l'architecture **Kappa**. Il permet l'analyse en temps réel d'annonces immobilières pour détecter les meilleures opportunités d'investissement.

## 🏗 Architecture

L'architecture est entièrement conteneurisée via Docker et se compose de 4 couches principales :

1.  **Ingestion Layer (Python Producers)** :
    *   `listings-producer` : Simule un flux d'annonces immobilières réaliste (via Faker) et les envoie dans Kafka (`real-estate-raw`).
    *   `georisks-ingester` : Récupère (simule) les données de risques naturels (API Géorisques) et les envoie dans Kafka.
    *   `financial-rates-producer` : Publie les taux d'intérêts actuels (Banque de France).

2.  **Messaging Layer (Apache Kafka)** :
    *   Sert de bus de données central et tampon persistant.
    *   Topics : `real-estate-raw`, `ref-georisques`, `financial-rates`.

3.  **Processing Layer (Apache Spark Structured Streaming)** :
    *   Consomme les annonces depuis Kafka.
    *   Enrichit les données avec les risques géographiques et les taux financiers.
    *   Calcule les indicateurs financiers : Rentabilité Brute, Cashflow, Score d'investissement.

4.  **Serving Layer (PostgreSQL)** :
    *   Stocke les annonces enrichies et les agrégats de marché dans un schéma en étoile (Star Schema).
    *   Tables : `fact_listings`, `dim_location`, `ref_taux`, etc.

## 🚀 Comment lancer le projet

### Pré-requis
*   Docker Desktop installé et démarré.

### Lancement

1.  **Démarrer l'infrastructure** :
    ```bash
    docker-compose up --build -d
    ```
    *Cette commande construit les images Python et télécharge les images Kafka, Spark, Postgres.*

2.  **Vérifier que tout tourne** :
    ```bash
    docker-compose ps
    ```
    Tous les conteneurs doivent être `Up`.

3.  **Soumettre le Job Spark** :
    Le conteneur `spark-processor` est configuré pour lancer le job automatiquement. Vous pouvez suivre ses logs :
    ```bash
    docker logs -f spark-processor
    ```

4.  **Vérifier les données dans PostgreSQL** :
    Connectez-vous à la base de données :
    ```bash
    docker exec -it postgres psql -U admin -d buyandrent
    ```
    Puis requêtez les données traitées :
    ```sql
    SELECT * FROM fact_listings ORDER BY date_creation DESC LIMIT 10;
    ```

## 📂 Structure du projet

```
.
├── app/
│   ├── producers/           # Scripts d'ingestion (Listings, Risques, Taux)
│   └── processors/          # Job Spark Streaming
├── sql/
│   └── init.sql             # Schéma de base de données
├── docker-compose.yml       # Orchestration
├── Dockerfile.producers     # Image pour les scripts Python
└── requirements.txt         # Dépendances Python
```

## 📝 Auteurs
*   Gael T (Étudiant Big Data)
