import time
import json
import os
import requests
from kafka import KafkaProducer

KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')
TOPIC_NAME = 'ref-georisques'

CITY_CODES = ['75056', '69123', '42218', '33063', '13055']

def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                linger_ms=100
            )
            return producer
        except Exception as e:
            print(f"Waiting for Kafka... {e}")
            time.sleep(5)

def fetch_risks(code_insee):
    # Simulated API call for stability in this demo environment
    # In a real scenario, use: requests.get(f"https://georisques.gouv.fr/api/v1/gaspar/risques?codeInsee={code_insee}")
    
    # Mock logic based on city
    mock_risks = {
        '75056': {'inondation': True, 'industriel': False, 'sismique': 1}, # Paris
        '69123': {'inondation': True, 'industriel': True, 'sismique': 2},  # Lyon
        '42218': {'inondation': False, 'industriel': True, 'sismique': 2}, # St Etienne
        '13055': {'inondation': True, 'industriel': True, 'sismique': 3},  # Marseille
    }
    
    risk_data = mock_risks.get(code_insee, {'inondation': False, 'industriel': False, 'sismique': 1})
    
    return {
        "code_insee": code_insee,
        "risque_inondation": risk_data['inondation'],
        "risque_industriel": risk_data['industriel'],
        "risque_sismique": risk_data['sismique']
    }

def main():
    producer = create_producer()
    print("Ingesting Georisks Data...")
    
    # Produce the reference data once (or periodically update)
    for code in CITY_CODES:
        data = fetch_risks(code)
        producer.send(TOPIC_NAME, key=code.encode('utf-8'), value=data)
        print(f"Sent risk data for {code}")
    
    producer.flush()
    print("Georisks ingestion complete. Sleeping...")
    
    # Keep container alive if it's a daemon service
    while True:
        time.sleep(3600)

if __name__ == "__main__":
    main()
