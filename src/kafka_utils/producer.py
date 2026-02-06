"""
Producteur Kafka pour l'ingestion des données en streaming
Envoie les données collectées vers les topics Kafka appropriés
"""
import json
import logging
import os
from typing import Any, Dict, List
from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

# Configuration Kafka
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

# Topics Kafka
TOPIC_LISTINGS = 'raw-listings'
TOPIC_GEORISKS = 'georisks'
TOPIC_RATES = 'rates'


class KafkaDataProducer:
    """
    Producteur Kafka pour envoyer les données vers les topics.
    """

    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS):
        """
        Initialise le producteur Kafka.

        Args:
            bootstrap_servers: Serveurs Kafka (ex: 'kafka:9092')
        """
        self.bootstrap_servers = bootstrap_servers
        self._producer = None

    def _get_producer(self) -> KafkaProducer:
        """Crée ou retourne le producteur Kafka existant."""
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                retry_backoff_ms=500,
            )
            logger.info("Producteur Kafka connecté à %s", self.bootstrap_servers)
        return self._producer

    def send(self, topic: str, data: Dict[str, Any], key: str = None) -> bool:
        """
        Envoie un message vers un topic Kafka.

        Args:
            topic: Nom du topic
            data: Données à envoyer (dict)
            key: Clé optionnelle pour le partitionnement

        Returns:
            True si succès, False sinon
        """
        try:
            producer = self._get_producer()
            future = producer.send(topic, value=data, key=key)
            # Attend la confirmation
            record_metadata = future.get(timeout=10)
            logger.debug(
                "Message envoyé: topic=%s partition=%s offset=%s",
                record_metadata.topic,
                record_metadata.partition,
                record_metadata.offset
            )
            return True
        except KafkaError as e:
            logger.error("Erreur Kafka lors de l'envoi: %s", e)
            return False

    def send_batch(self, topic: str, data_list: List[Dict[str, Any]], key_field: str = None) -> int:
        """
        Envoie un batch de messages vers un topic Kafka.

        Args:
            topic: Nom du topic
            data_list: Liste de données à envoyer
            key_field: Champ à utiliser comme clé (optionnel)

        Returns:
            Nombre de messages envoyés avec succès
        """
        success_count = 0
        for data in data_list:
            key = data.get(key_field) if key_field else None
            if self.send(topic, data, key):
                success_count += 1

        # Flush pour s'assurer que tous les messages sont envoyés
        self._get_producer().flush()

        logger.info(
            "Batch envoyé: %s/%s messages vers topic '%s'",
            success_count, len(data_list), topic
        )
        return success_count

    def close(self):
        """Ferme le producteur Kafka."""
        if self._producer:
            self._producer.close()
            self._producer = None
            logger.info("Producteur Kafka fermé")


def send_listings_to_kafka(listings: List[Dict[str, Any]], **context) -> int:
    """
    Fonction Airflow pour envoyer les annonces vers Kafka.

    Args:
        listings: Liste des annonces à envoyer

    Returns:
        Nombre de messages envoyés
    """
    ti = context.get('ti')
    if ti and not listings:
        listings = ti.xcom_pull(task_ids='collect_listings', key='raw_listings')

    if not listings:
        logger.warning("Aucune annonce à envoyer vers Kafka")
        return 0

    producer = KafkaDataProducer()
    try:
        count = producer.send_batch(TOPIC_LISTINGS, listings, key_field='code_insee')
        logger.info("[OK] %s annonces envoyées vers Kafka topic '%s'", count, TOPIC_LISTINGS)

        if ti:
            ti.xcom_push(key='kafka_listings_count', value=count)

        return count
    finally:
        producer.close()


def send_georisks_to_kafka(georisks: List[Dict[str, Any]] = None, **context) -> int:
    """
    Fonction Airflow pour envoyer les risques géographiques vers Kafka.

    Args:
        georisks: Liste des risques à envoyer

    Returns:
        Nombre de messages envoyés
    """
    ti = context.get('ti')
    if ti and not georisks:
        georisks = ti.xcom_pull(task_ids='collect_georisks', key='georisks')

    if not georisks:
        logger.warning("Aucun risque géographique à envoyer vers Kafka")
        return 0

    producer = KafkaDataProducer()
    try:
        count = producer.send_batch(TOPIC_GEORISKS, georisks, key_field='code_insee')
        logger.info("[OK] %s risques envoyés vers Kafka topic '%s'", count, TOPIC_GEORISKS)

        if ti:
            ti.xcom_push(key='kafka_georisks_count', value=count)

        return count
    finally:
        producer.close()


def send_rates_to_kafka(rates: Dict[str, Any] = None, **context) -> int:
    """
    Fonction Airflow pour envoyer les taux vers Kafka.

    Args:
        rates: Dictionnaire des taux à envoyer

    Returns:
        1 si succès, 0 sinon
    """
    ti = context.get('ti')
    if ti and not rates:
        rates = ti.xcom_pull(task_ids='collect_rates', key='rates')

    if not rates:
        logger.warning("Aucun taux à envoyer vers Kafka")
        return 0

    producer = KafkaDataProducer()
    try:
        if producer.send(TOPIC_RATES, rates):
            logger.info("[OK] Taux envoyés vers Kafka topic '%s'", TOPIC_RATES)

            if ti:
                ti.xcom_push(key='kafka_rates_sent', value=True)

            return 1
        return 0
    finally:
        producer.close()
