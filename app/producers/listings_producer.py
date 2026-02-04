import time
import json
import random
import uuid
import os
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker
from faker.providers import BaseProvider

# Configuration
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')
TOPIC_NAME = 'real-estate-raw'

# Custom Provider for Real Estate specific data
class RealEstateProvider(BaseProvider):
    def dpe_class(self):
        return random.choice(['A', 'B', 'C', 'D', 'E', 'F', 'G'])

    def city_profile(self):
        profiles = [
            {'code_insee': '75056', 'ville': 'Paris', 'price_m2_avg': 10500, 'lat': 48.8566, 'lon': 2.3522},
            {'code_insee': '69123', 'ville': 'Lyon', 'price_m2_avg': 5200, 'lat': 45.7640, 'lon': 4.8357},
            {'code_insee': '42218', 'ville': 'Saint-Étienne', 'price_m2_avg': 1200, 'lat': 45.4397, 'lon': 4.3872},
            {'code_insee': '33063', 'ville': 'Bordeaux', 'price_m2_avg': 4600, 'lat': 44.8378, 'lon': -0.5792},
            {'code_insee': '13055', 'ville': 'Marseille', 'price_m2_avg': 3500, 'lat': 43.2965, 'lon': 5.3698}
        ]
        return random.choice(profiles)

fake = Faker('fr_FR')
fake.add_provider(RealEstateProvider)

def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                linger_ms=5
            )
            print(f"Connected to Kafka at {KAFKA_BROKER}")
            return producer
        except Exception as e:
            print(f"Waiting for Kafka... {e}")
            time.sleep(5)

def generate_listing():
    profile = fake.city_profile()
    surface = random.randint(15, 120)
    
    # Introduce some variance in price
    price_variance = random.uniform(0.85, 1.15)
    base_price = surface * profile['price_m2_avg'] * price_variance
    
    # Price anomalies (rare)
    if random.random() < 0.01: 
        base_price = base_price * 0.5 # Super cheap (suspicious)
    
    listing = {
        "id": str(uuid.uuid4()),
        "timestamp": datetime.now().isoformat(),
        "code_insee": profile['code_insee'],
        "ville": profile['ville'],
        "location": {
            "lat": profile['lat'] + random.uniform(-0.01, 0.01),
            "lon": profile['lon'] + random.uniform(-0.01, 0.01)
        },
        "surface": surface,
        "nb_pieces": max(1, surface // 25),
        "price": round(base_price, 2),
        "dpe": fake.dpe_class(),
        "description": fake.text(max_nb_chars=200),
        "contact_email": fake.email() if random.random() > 0.1 else f"contact.{fake.phone_number()}@test.com" # Dirty data sometimes
    }
    return listing

def main():
    producer = create_producer()
    print("Starting listing generation...")
    
    while True:
        listing = generate_listing()
        producer.send(TOPIC_NAME, key=listing['code_insee'].encode('utf-8'), value=listing)
        print(f"Sent: {listing['ville']} - {listing['surface']}m2 - {listing['price']}€")
        
        # Simulate variable throughput
        time.sleep(random.uniform(0.5, 3.0))

if __name__ == "__main__":
    main()
