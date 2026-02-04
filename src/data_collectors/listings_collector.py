"""
Collecteur d'annonces immobilières
Simule la collecte de données depuis des APIs immobilières
"""
import json
import uuid
import random
from datetime import datetime
from faker import Faker

fake = Faker('fr_FR')

# Liste des villes avec leurs codes INSEE
CITIES = {
    '75056': 'Paris',
    '69123': 'Lyon',
    '13055': 'Marseille',
    '33063': 'Bordeaux',
    '42218': 'Saint-Étienne'
}

def generate_listing():
    """Génère une annonce immobilière fictive"""
    code_insee = random.choice(list(CITIES.keys()))
    surface = random.randint(20, 150)
    price_per_m2 = random.randint(2000, 12000)
    
    return {
        'id': str(uuid.uuid4()),
        'timestamp': datetime.now().isoformat(),
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
    Collecte les annonces immobilières
    Stocke les données dans XCom pour les tasks suivantes
    """
    print("Debut de la collecte des annonces...")
    
    # Génère un batch d'annonces
    listings = [generate_listing() for _ in range(10)]
    
    print(f"{len(listings)} annonces collectees")
    
    # Stocke dans XCom pour les tasks suivantes
    context['ti'].xcom_push(key='raw_listings', value=listings)
    
    return listings
