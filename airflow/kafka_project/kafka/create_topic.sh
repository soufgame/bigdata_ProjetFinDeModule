
---

## 🛠️ `kafka/create_topic.sh`

```bash
#!/bin/bash

KAFKA_CONTAINER=kafka
TOPIC_NAME=news_articles

docker exec -it $KAFKA_CONTAINER kafka-topics.sh \
  --create \
  --topic $TOPIC_NAME \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
