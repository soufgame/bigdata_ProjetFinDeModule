import time
import csv
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_HOST = "kafka:9092"
TOPIC = "news_articles"
CSV_FILE = "data/articles.csv"  # ton fichier CSV dans le même dossier que producer.py

# Attente que Kafka soit prêt
while True:
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_HOST)
        print("Connected to Kafka!")
        break
    except NoBrokersAvailable:
        print("Kafka not ready yet, retrying in 5 seconds...")
        time.sleep(5)

# Lecture du CSV et envoi des messages
with open(CSV_FILE, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)  # lire avec les noms de colonnes
    for row in reader:
        # Convertir la ligne en string JSON ou texte simple
        message = str(row).encode("utf-8")
        producer.send(TOPIC, message)
        producer.flush()
        print(f"Message sent: {message.decode()}")
        time.sleep(1)  # pause 1 seconde entre chaque article (optionnel)
