"""
Module d'enrichissement des données
Combine les annonces avec les risques et calcule les indicateurs financiers
"""

import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def enrich_listings(**context):
    """
    Enrichit les annonces avec:
    - Les risques géographiques
    - Les calculs financiers (rentabilité, cashflow, score)

    Effectue des contrôles de cohérence sur les jointures et les valeurs.
    """
    ti = context['ti']
    execution_date = context.get("execution_date")
    batch_date = execution_date.date().isoformat() if execution_date else None
    logger.info("🔄 Début de l'enrichissement des données pour le batch %s", batch_date)

    # Récupère les données des tasks précédentes
    listings: List[Dict[str, Any]] = ti.xcom_pull(
        task_ids='collect_listings', key='raw_listings'
    )
    georisks: List[Dict[str, Any]] = ti.xcom_pull(
        task_ids='collect_georisks', key='georisks'
    )
    rates: Dict[str, Any] = ti.xcom_pull(
        task_ids='collect_rates', key='rates'
    )

    if not listings:
        raise ValueError("Aucune annonce en entrée pour l'enrichissement")
    if rates is None:
        raise ValueError("Aucun taux financier disponible pour l'enrichissement")

    # Crée un dictionnaire de risques par code_insee
    risks_by_insee = {r['code_insee']: r for r in (georisks or [])}

    # Taux d'intérêt (utilise le taux nominal 20 ans fourni par le collecteur)
    interest_rate = float(rates['taux_nominal_20ans']) / 100.0
    months = 240  # 20 ans
    monthly_rate = interest_rate / 12.0 if interest_rate > 0 else 0.0

    enriched_listings: List[Dict[str, Any]] = []
    listings_without_risks = 0

    for listing in listings:
        code_insee = listing['code_insee']
        price = listing['price']

        # Ajoute les risques
        risk = risks_by_insee.get(code_insee, {})
        has_risk_data = bool(risk)
        if not has_risk_data:
            listings_without_risks += 1

        listing['risque_inondation'] = risk.get('risque_inondation', False)
        listing['risque_industriel'] = risk.get('risque_industriel', False)
        listing['risque_sismique'] = risk.get('risque_sismique', 0)
        listing['risques_complets'] = has_risk_data

        # Calculs financiers
        if price <= 0:
            logger.warning(
                "Prix <= 0 pour l'annonce %s, annonce ignorée pour les calculs financiers",
                listing.get('id'),
            )
            continue

        # Mensualité d'emprunt
        if interest_rate > 0:
            factor = monthly_rate / (1 - (1 + monthly_rate) ** (-months))
            mensualite = price * factor
        else:
            mensualite = price / months

        # Loyer estimé (5% de rendement brut annuel)
        loyer_estime = price * 0.05 / 12

        # Cashflow mensuel
        cashflow = loyer_estime - mensualite - 150  # 150€ charges/taxes

        # Rentabilité brute
        rentabilite_brute = (loyer_estime * 12 / price) * 100

        # Score d'investissement (règle simple)
        if cashflow > 0:
            score = 8.0
        elif rentabilite_brute > 6:
            score = 6.0
        else:
            score = 4.0

        # Enrichit l'annonce
        listing['batch_date'] = batch_date or listing.get('batch_date')
        listing['loyer_estime'] = round(loyer_estime, 2)
        listing['mensualite'] = round(mensualite, 2)
        listing['cashflow'] = round(cashflow, 2)
        listing['rentabilite_brute'] = round(rentabilite_brute, 2)
        listing['score_investissement'] = score

        enriched_listings.append(listing)

    if not enriched_listings:
        raise ValueError("Aucune annonce enrichie, vérifier les données d'entrée")

    logger.info("[OK] %s annonces enrichies", len(enriched_listings))
    if listings_without_risks > 0:
        logger.warning(
            "%s annonces sur %s n'ont pas de données de risques associées",
            listings_without_risks,
            len(listings),
        )

    # Stocke pour le loader
    ti.xcom_push(key='enriched_listings', value=enriched_listings)

    return enriched_listings
