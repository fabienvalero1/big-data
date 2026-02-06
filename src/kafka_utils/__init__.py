"""
Module Kafka pour l'ingestion et la consommation de données en streaming
"""
from .producer import KafkaDataProducer, send_listings_to_kafka, send_georisks_to_kafka, send_rates_to_kafka
from .consumer import KafkaDataConsumer, consume_and_save

__all__ = [
    'KafkaDataProducer',
    'KafkaDataConsumer',
    'send_listings_to_kafka',
    'send_georisks_to_kafka',
    'send_rates_to_kafka',
    'consume_and_save',
]
