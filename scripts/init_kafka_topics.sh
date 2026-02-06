#!/bin/bash
# Script pour initialiser les topics Kafka
# Usage: docker exec kafka bash /scripts/init_kafka_topics.sh

KAFKA_BROKER="localhost:9092"

echo "Création des topics Kafka..."

# Topic pour les annonces immobilières
kafka-topics --create --if-not-exists \
  --bootstrap-server $KAFKA_BROKER \
  --topic raw-listings \
  --partitions 3 \
  --replication-factor 1

# Topic pour les risques géographiques
kafka-topics --create --if-not-exists \
  --bootstrap-server $KAFKA_BROKER \
  --topic georisks \
  --partitions 1 \
  --replication-factor 1

# Topic pour les taux financiers
kafka-topics --create --if-not-exists \
  --bootstrap-server $KAFKA_BROKER \
  --topic rates \
  --partitions 1 \
  --replication-factor 1

echo "Topics créés avec succès!"

# Liste des topics
echo ""
echo "Liste des topics Kafka:"
kafka-topics --list --bootstrap-server $KAFKA_BROKER
