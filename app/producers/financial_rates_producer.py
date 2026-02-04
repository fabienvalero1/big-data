import time
import json
import os
from kafka import KafkaProducer

KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')
TOPIC_NAME = 'financial-rates'

def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            return producer
        except Exception:
            time.sleep(5)

def main():
    producer = create_producer()
    print("Publishing Financial Rates...")
    
    while True:
        # Simulate rates (potentially fluctuating)
        rates = {
            "timestamp": time.time(),
            "taux_interet_moyen": 3.85,
            "taux_usure": 6.15,
            "duree_ref_annee": 20
        }
        
        producer.send(TOPIC_NAME, value=rates)
        print(f"Sent rates: {rates}")
        
        # Slow changing dimension - update every minute for demo
        time.sleep(60)

if __name__ == "__main__":
    main()
