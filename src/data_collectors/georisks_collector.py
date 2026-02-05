"""
Collecteur de donnees de risques geographiques
Appelle l'API Georisques
"""

import logging
import os
import time
from typing import Any, Dict, List

import requests

logger = logging.getLogger(__name__)

# Villes cibles (Codes INSEE)
# 75056: Paris, 69123: Lyon, 13055: Marseille, 33063: Bordeaux, 42218: Saint-Étienne
TARGET_CITIES = ['75056', '69123', '13055', '33063', '42218']
GEORISKS_BASE_URL = os.environ.get(
    "GEORISKS_API_BASE_URL",
    "https://georisques.gouv.fr/api/v1/gaspar/risques",
)


def _parse_sismicity(raw_value: Any) -> int:
    """
    Extrait un entier raisonnable à partir du classement sismique (ex: '2 (Faible)').
    Retourne 0 si non interprétable.
    """
    if isinstance(raw_value, int):
        return max(0, min(raw_value, 5))
    if not isinstance(raw_value, str):
        return 0
    try:
        level = int(raw_value.split()[0])
        return max(0, min(level, 5))
    except Exception:
        return 0


def collect_georisks(**context):
    """
    Collecte les donnees de risques geographiques via l'API Georisques.
    Utilise GEORISKS_API_BASE_URL si défini dans l'environnement.
    """
    ti = context["ti"]
    logger.info("Début de la collecte des risques géographiques (API réelle)...")

    risks: List[Dict[str, Any]] = []
    cities_without_data: List[str] = []

    for code_insee in TARGET_CITIES:
        try:
            url = f"{GEORISKS_BASE_URL}?code_insee={code_insee}"
            logger.info("Appel API Georisques pour %s", code_insee)
            response = requests.get(url, timeout=10)

            if response.status_code == 200:
                data = response.json()
                if 'data' in data and data['data']:
                    city_risks = data['data'][0]
                    item = {
                        'code_insee': code_insee,
                        'risque_inondation': any(
                            r.get('libelle_risque_long') == 'Inondation' for r in data['data']
                        ),
                        'risque_industriel': any(
                            r.get('libelle_risque_long') == 'Industriel' for r in data['data']
                        ),
                        'risque_sismique': _parse_sismicity(
                            city_risks.get('classement_sismique')
                        ),
                    }
                    risks.append(item)
                else:
                    logger.warning("Pas de données de risque pour %s", code_insee)
                    cities_without_data.append(code_insee)
            else:
                logger.error(
                    "Erreur API Georisques %s pour %s", response.status_code, code_insee
                )

            # Petit délai pour être poli avec l'API
            time.sleep(0.5)

        except Exception as e:
            logger.exception("Exception lors de l'appel API pour %s: %s", code_insee, e)

    logger.info("Risques collectés pour %s communes", len(risks))
    if cities_without_data:
        logger.warning(
            "Aucune donnée de risque pour les communes suivantes: %s",
            ", ".join(cities_without_data),
        )

    # Stocke dans XCom
    ti.xcom_push(key='georisks', value=risks)

    return risks
