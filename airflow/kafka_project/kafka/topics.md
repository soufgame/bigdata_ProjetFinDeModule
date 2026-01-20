# Kafka Topics Documentation

## Topic Name
**news_articles**

## Description
Ce topic contient les articles collectés depuis des sources de presse (NewsAPI).
Chaque message représente un article envoyé par le producteur Kafka au format JSON.

## Configuration
- **Partitions**: 1  
- **Replication factor**: 1  
- **Retention**: par défaut (config Kafka)

## Message Format (JSON)
```json
{
  "source": "newsapi",
  "title": "University students facing course cold spots",
  "description": "New data analysis suggests courses like artificial intelligence...",
  "content": "...",
  "author": "Nathan Standley",
  "published_at": "2025-12-16T02:24:03Z",
  "url": "https://www.bbc.com/news/articles/cnv26103d1go",
  "keywords": ["AI", "automation", "jobs"],
  "ingested_at": "2026-01-04T20:40:13.853520"
}
