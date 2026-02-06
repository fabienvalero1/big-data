"""
Consommateur Kafka pour lire les données des topics
et les préparer pour le traitement Spark
"""
import json
import logging
import os
from typing import Any, Dict, List, Optional
from kafka import KafkaConsumer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

# Configuration Kafka
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

# Topics Kafka
TOPIC_LISTINGS = 'raw-listings'
TOPIC_GEORISKS = 'georisks'
TOPIC_RATES = 'rates'


class KafkaDataConsumer:
    """
    Consommateur Kafka pour lire les données des topics.
    """

    def __init__(
        self,
        topics: List[str],
        group_id: str = 'bigdata-consumer-group',
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset: str = 'earliest'
    ):
        """
        Initialise le consommateur Kafka.

        Args:
            topics: Liste des topics à consommer
            group_id: ID du groupe de consommateurs
            bootstrap_servers: Serveurs Kafka
            auto_offset_reset: Position de départ ('earliest' ou 'latest')
        """
        self.topics = topics
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        self.auto_offset_reset = auto_offset_reset
        self._consumer = None

    def _get_consumer(self) -> KafkaConsumer:
        """Crée ou retourne le consommateur Kafka existant."""
        if self._consumer is None:
            self._consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset=self.auto_offset_reset,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=10000,  # Timeout après 10 secondes sans message
                enable_auto_commit=True,
            )
            logger.info(
                "Consommateur Kafka connecté: topics=%s, group=%s",
                self.topics, self.group_id
            )
        return self._consumer

    def consume(self, max_messages: int = 1000) -> Dict[str, List[Dict[str, Any]]]:
        """
        Consomme les messages de tous les topics.

        Args:
            max_messages: Nombre maximum de messages à consommer

        Returns:
            Dictionnaire {topic: [messages]}
        """
        consumer = self._get_consumer()
        messages_by_topic: Dict[str, List[Dict[str, Any]]] = {t: [] for t in self.topics}
        total_count = 0

        try:
            for message in consumer:
                topic = message.topic
                value = message.value

                if topic in messages_by_topic:
                    messages_by_topic[topic].append(value)
                    total_count += 1

                    logger.debug(
                        "Message reçu: topic=%s partition=%s offset=%s",
                        topic, message.partition, message.offset
                    )

                if total_count >= max_messages:
                    logger.info("Limite de messages atteinte (%s)", max_messages)
                    break

        except KafkaError as e:
            logger.error("Erreur Kafka lors de la consommation: %s", e)

        for topic, msgs in messages_by_topic.items():
            logger.info("Topic '%s': %s messages consommés", topic, len(msgs))

        return messages_by_topic

    def consume_topic(self, topic: str, max_messages: int = 1000) -> List[Dict[str, Any]]:
        """
        Consomme les messages d'un topic spécifique.

        Args:
            topic: Nom du topic
            max_messages: Nombre maximum de messages

        Returns:
            Liste des messages
        """
        if topic not in self.topics:
            logger.warning("Topic '%s' non souscrit", topic)
            return []

        messages = self.consume(max_messages)
        return messages.get(topic, [])

    def close(self):
        """Ferme le consommateur Kafka."""
        if self._consumer:
            self._consumer.close()
            self._consumer = None
            logger.info("Consommateur Kafka fermé")


def consume_and_save(output_dir: str, **context) -> Dict[str, str]:
    """
    Consomme les données Kafka et les sauvegarde en fichiers JSON
    pour le traitement Spark.

    Args:
        output_dir: Répertoire de sortie pour les fichiers JSON

    Returns:
        Dictionnaire avec les chemins des fichiers créés
    """
    import os
    from datetime import datetime

    ti = context.get('ti')
    execution_date = context.get('execution_date')
    batch_id = execution_date.strftime('%Y%m%d_%H%M%S') if execution_date else datetime.now().strftime('%Y%m%d_%H%M%S')

    # Crée le répertoire de sortie
    batch_dir = os.path.join(output_dir, f"batch_{batch_id}")
    os.makedirs(batch_dir, exist_ok=True)

    logger.info("📥 Consommation des données Kafka pour le batch %s", batch_id)

    consumer = KafkaDataConsumer(
        topics=[TOPIC_LISTINGS, TOPIC_GEORISKS, TOPIC_RATES],
        group_id=f'spark-consumer-{batch_id}'
    )

    try:
        messages = consumer.consume(max_messages=500)

        output_files = {}

        # Sauvegarde les annonces
        listings = messages.get(TOPIC_LISTINGS, [])
        if listings:
            listings_path = os.path.join(batch_dir, 'listings.json')
            with open(listings_path, 'w') as f:
                for listing in listings:
                    f.write(json.dumps(listing) + '\n')
            output_files['listings'] = listings_path
            logger.info("[OK] %s annonces sauvegardées: %s", len(listings), listings_path)

        # Sauvegarde les risques
        georisks = messages.get(TOPIC_GEORISKS, [])
        if georisks:
            georisks_path = os.path.join(batch_dir, 'georisks.json')
            with open(georisks_path, 'w') as f:
                for risk in georisks:
                    f.write(json.dumps(risk) + '\n')
            output_files['georisks'] = georisks_path
            logger.info("[OK] %s risques sauvegardés: %s", len(georisks), georisks_path)

        # Sauvegarde les taux (dernier message)
        rates_list = messages.get(TOPIC_RATES, [])
        if rates_list:
            rates = rates_list[-1]  # Prend le dernier taux
            rates_path = os.path.join(batch_dir, 'rates.json')
            with open(rates_path, 'w') as f:
                json.dump(rates, f)
            output_files['rates'] = rates_path
            logger.info("[OK] Taux sauvegardés: %s", rates_path)

        # Stocke les chemins dans XCom
        if ti:
            ti.xcom_push(key='kafka_batch_dir', value=batch_dir)
            ti.xcom_push(key='kafka_output_files', value=output_files)

        logger.info("📁 Données Kafka sauvegardées dans: %s", batch_dir)
        return output_files

    finally:
        consumer.close()
