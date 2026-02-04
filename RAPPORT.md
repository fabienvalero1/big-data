# Rapport de Projet Big Data : Buy & Rent

## 1. Problématique Métier

Dans un marché immobilier dynamique, les investisseurs ont besoin d'identifier rapidement les opportunités rentables. Ce projet vise à construire une plateforme capable de :

- **Collecter** des données hétérogènes (annonces, risques, taux)
- **Transformer** ces données pour calculer des indicateurs clés
- **Stocker** les résultats dans un modèle analytique
- **Visualiser** les tendances via des dashboards

## 2. Architecture Globale

Nous utilisons une architecture **ETL orchestrée par Airflow** :

| Composant | Technologie | Rôle |
|-----------|-------------|------|
| Orchestration | Apache Airflow | Planification et exécution des pipelines |
| Base de données | PostgreSQL | Stockage des données (Data Warehouse) |
| Logs | Loki + Promtail | Collecte et agrégation des logs |
| Visualisation | Grafana | Dashboards et monitoring |

### Avantages de cette architecture :
- **Reproductibilité** : Les DAGs Airflow définissent clairement le pipeline
- **Observabilité** : Loki/Grafana permettent de monitorer l'exécution
- **Scalabilité** : Chaque composant peut évoluer indépendamment

## 3. Pipeline de Données

Le DAG `buy_and_rent_pipeline` s'exécute toutes les heures et comprend 5 tâches :

```
[Collect Listings] ──┐
[Collect Georisks] ──┼──> [Enrich Listings] ──> [Load to PostgreSQL]
[Collect Rates]    ──┘
```

### Détail des tâches :

1. **collect_listings** : Génère des annonces immobilières (simulation)
2. **collect_georisks** : Récupère les risques naturels par commune
3. **collect_rates** : Récupère les taux d'intérêt actuels
4. **enrich_listings** : Calcule rentabilité, cashflow, score
5. **load_to_postgres** : Insère les données dans `fact_listings`

## 4. Modèle de Données (Star Schema)

| Table | Type | Description |
|-------|------|-------------|
| `fact_listings` | Fait | Annonces enrichies avec indicateurs |
| `dim_location` | Dimension | Informations géographiques |
| `ref_georisques` | Référence | Risques par commune |
| `ref_taux` | Référence | Taux financiers |

## 5. Résultats et Conclusions

La plateforme permet :
- ✅ Collecte automatisée de données hétérogènes
- ✅ Calcul d'indicateurs d'investissement en temps quasi-réel
- ✅ Stockage structuré pour analyse BI
- ✅ Monitoring complet via Grafana

### Évolutions possibles :
- Intégration d'APIs réelles (SeLoger, LeBonCoin)
- Ajout de modèles ML pour prédiction de prix
- Alertes automatiques sur opportunités
