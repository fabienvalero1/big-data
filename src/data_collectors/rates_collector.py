"""
Collecteur de taux financiers
Simule les données de la Banque de France
"""
from datetime import date
import logging
import os

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

EURIBOR_URL = os.environ.get(
    "EURIBOR_3M_URL",
    "https://www.euribor-rates.eu/en/current-euribor-rates/3-months-euribor-rate/",
)


def _is_rate_reasonable(value: float) -> bool:
    """Vérifie qu'un taux est dans une plage réaliste [0, 20]%."""
    return 0 <= value <= 20


def collect_rates(**context):
    """
    Collecte les taux d'intérêt actuels via scraping ou simulation améliorée.
    Source: Euribor Rates (ou fallback).
    """
    ti = context["ti"]
    logger.info("Début de la collecte des taux financiers (scraping)...")

    euribor_3m = None

    # Tentative de scraping (Euribor 3 mois)
    try:
        logger.info("Scraping de %s...", EURIBOR_URL)

        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(EURIBOR_URL, headers=headers, timeout=10)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            card_body = soup.find('div', class_='card-body')
            if card_body:
                val_text = (
                    card_body.find('table')
                    .findAll('td')[-1]
                    .text.strip()
                    .replace('%', '')
                )
                euribor_3m = float(val_text)
                logger.info("Taux Euribor 3m trouvé: %.3f%%", euribor_3m)
            else:
                logger.warning("Structure HTML non reconnue pour le scraping Euribor")
        else:
            logger.error("Erreur HTTP %s lors du scraping Euribor", response.status_code)

    except Exception as e:
        logger.exception("Erreur lors du scraping Euribor: %s", e)

    # Fallback si échec ou valeur aberrante
    if euribor_3m is None or not _is_rate_reasonable(euribor_3m):
        logger.warning(
            "Utilisation de la valeur par défaut pour Euribor 3m (fallback), valeur actuelle: %s",
            euribor_3m,
        )
        euribor_3m = 3.5

    # Marge banque (simulée)
    taux_client = round(euribor_3m + 1.2, 2)
    taux_usure = round(taux_client + 2.0, 2)  # estimation simple

    # Contrôle de cohérence
    if not all(
        _is_rate_reasonable(v) for v in (euribor_3m, taux_client, taux_usure)
    ):
        raise ValueError(
            f"Taux financiers incohérents: euribor={euribor_3m}, client={taux_client}, usure={taux_usure}"
        )

    rates = {
        'date_valeur': date.today().isoformat(),
        'taux_euribor_3m': euribor_3m,
        'taux_nominal_20ans': taux_client,
        'taux_usure': taux_usure,
    }

    logger.info(
        "Taux collectés: Euribor 3m=%.3f%%, nominal 20 ans=%.2f%%, usure=%.2f%%",
        euribor_3m,
        taux_client,
        taux_usure,
    )

    # Stocke dans XCom
    ti.xcom_push(key='rates', value=rates)

    return rates
