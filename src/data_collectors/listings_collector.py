"""
Collecteur d'annonces immobilières
Simule la collecte de données depuis des APIs immobilières
"""
import json
import uuid
import random
import logging
from datetime import datetime
from faker import Faker

fake = Faker('fr_FR')
logger = logging.getLogger(__name__)

# Liste des villes avec leurs codes INSEE
CITIES = {
    '75056': 'Paris',
    '69123': 'Lyon',
    '13055': 'Marseille',
    '33063': 'Bordeaux',
    '42218': 'Saint-Étienne'
}


def _is_listing_valid(listing: dict) -> bool:
    """
    Règles simples de qualité des données pour une annonce.
    - surface dans [10, 500] m2
    - prix positif et raisonnable (< 5M€)
    """
    surface = listing.get('surface') or 0
    price = listing.get('price') or 0

    if surface <= 10 or surface > 500:
        return False
    if price <= 0 or price > 5_000_000:
        return False
    return True


def generate_listing(batch_date: str | None = None) -> dict:
    """Génère une annonce immobilière fictive"""
    code_insee = random.choice(list(CITIES.keys()))
    surface = random.randint(20, 150)
    price_per_m2 = random.randint(2000, 12000)

    return {
        'id': str(uuid.uuid4()),
        'timestamp': datetime.now().isoformat(),
        'batch_date': batch_date,
        'code_insee': code_insee,
        'ville': CITIES[code_insee],
        'surface': surface,
        'nb_pieces': max(1, surface // 25),
        'price': surface * price_per_m2,
        'dpe': random.choice(['A', 'B', 'C', 'D', 'E', 'F', 'G']),
        'description': fake.text(max_nb_chars=200)
    }


def collect_listings(**context):
    """
    Collecte les annonces immobilières.
    Applique des règles de qualité basiques et logue les volumes.
    Stocke les données dans XCom pour les tasks suivantes.
    """
    ti = context["ti"]
    execution_date = context.get("execution_date")
    batch_date = execution_date.date().isoformat() if execution_date else None

    logger.info("Début de la collecte des annonces pour le batch %s", batch_date)

    # Génère un batch brut d'annonces
    raw_listings = [generate_listing(batch_date=batch_date) for _ in range(10)]
    total = len(raw_listings)

    # Filtrage des annonces aberrantes
    valid_listings = []
    rejected = []
    for listing in raw_listings:
        if _is_listing_valid(listing):
            valid_listings.append(listing)
        else:
            rejected.append(listing)

    logger.info(
        "Annonces collectées: %s brutes, %s valides, %s rejetées pour qualité",
        total,
        len(valid_listings),
        len(rejected),
    )

    # Stocke dans XCom pour les tasks suivantes
    ti.xcom_push(key='raw_listings', value=valid_listings)

    # On retourne aussi les annonces valides
    return valid_listings
