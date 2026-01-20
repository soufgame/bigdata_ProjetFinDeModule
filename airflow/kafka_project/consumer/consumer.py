import time
from kafka import KafkaConsumer
from pymongo import MongoClient
from kafka.errors import NoBrokersAvailable

KAFKA_HOST = "kafka:9092"
TOPIC = "news_articles"  # ← doit correspondre exactement au topic
MONGO_HOST = "mongodb"
MONGO_DB = "news_db"
MONGO_COLLECTION = "raw_articles"

# Attente que Kafka soit prêt
while True:
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_HOST,
            auto_offset_reset='earliest',  # lire tous les messages existants
            group_id='news_group'
        )
        print("Connected to Kafka!")
        break
    except NoBrokersAvailable:
        print("Kafka not ready yet, retrying in 5 seconds...")
        time.sleep(5)

# Connexion MongoDB
client = MongoClient(MONGO_HOST, 27017)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

print("Consumer started, waiting for messages...")

# Boucle infinie pour lire et stocker tous les messages
for message in consumer:
    data = message.value.decode('utf-8')  # convertir bytes en string
    print(f"Received: {data}")
    # Insérer dans MongoDB
    try:
        collection.insert_one(eval(data))  # transformer string en dict
        print("Inserted article into MongoDB")
    except Exception as e:
        print(f"MongoDB insert error: {e}")
