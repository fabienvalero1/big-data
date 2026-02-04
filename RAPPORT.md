# Rapport de Projet Big Data : Buy & Rent

## 1. Problématique Métier
Dans un marché immobilier tendu dynamique, la réactivité est la clé. Les investisseurs ont besoin d'identifier les opportunités rentables (cashflow positif, haute rentabilité) **en temps réel**, dès la publication des annonces.

Les solutions traditionnelles (batch quotidien) sont trop lentes. Ce projet vise à mettre en place une plateforme Big Data capable de :
- **Ingérer** des flux continus d'annonces.
- **Enrichir** ces annonces instantanément avec des données contextuelles (Risques naturels, Taux d'emprunt).
- **Calculer** des indicateurs de décision (Rentabilité, Cashflow).
- **Stocker** les données pour analyse immédiate.

## 2. Architecture Globale
Nous avons opté pour un **Architecture Kappa**, simplifiant la maintenance en traitant tout (batch et temps réel) via un moteur unique de streaming.

**Composants Clés :**
*   **Ingestion** : Scripts Python (Producers) simulant des API externes.
*   **Data Lake** : **Apache Kafka** avec rétention des messages. Dans l'architecture Kappa, Kafka sert de source de vérité ("log-based data lake") car il stocke les événements bruts de façon persistante et rejouable.
*   **Processing** : **Apache Spark Structured Streaming** pour le calcul distribué.
*   **Serving** : **PostgreSQL** pour le stockage structuré et le requêtage SQL.

## 3. Pipeline de Données
Le flux de données suit les étapes suivantes :

1.  **Ingestion** :
    *   `listings-producer` -> Topic `real-estate-raw` (Offres immobilières).
    *   `georisks-ingester` -> Topic `ref-georisques` (Données inondation/sismique).
    *   `financial-rates-producer` -> Table `ref_taux` (Taux Banque de France).
2.  **Data Lake (Kafka)** :
    *   Les topics Kafka stockent les données brutes de façon persistante (configurable via `retention.ms`).
    *   Permet le "replay" des données pour retraitement.
3.  **Traitement (Spark)** :
    *   Lecture du stream Kafka `real-estate-raw`.
    *   Jointure avec les référentiels (Risques, Taux).
    *   Calcul du *Cashflow* et du *Score d'Investissement*.
4.  **Stockage (PostgreSQL)** :
    *   Insertion en base SQL (`fact_listings`) pour exploitation analytique.

## 4. Modèle de Données (Star Schema)
Pour faciliter l'analyse décisionnelle (BI), nous utilisons un modèle en étoile dans PostgreSQL :

*   **Table de Fait** : `fact_listings` (Chaque ligne est une annonce enrichie avec prix, renta, cashflow).
*   **Tables de Dimension** :
    *   `dim_location` : Données géographiques (Ville, Zone tendue).
    *   `dim_properties` : Caractéristiques du bien (Surface, DPE).

## 5. Résultats et Conclusions
La plateforme déployée permet :
*   Une centralisation des données hétérogènes.
*   Un calcul de rentabilité fiable prenant en compte les risques et les taux actuels.
*   Une persistance robuste (Data Lake + Base de données).

Cette architecture est scalable : l'ajout de nouveaux nœuds Spark ou Kafka permettrait de gérer des volumes massifs sans refonte du code.
