# MongoDB Schema Documentation

## Database
**news_db**

## Collection
**raw_articles**

## Description
Cette collection contient les données brutes des articles consommés depuis Kafka.
Aucune transformation avancée n’est appliquée avant le stockage.

## Document Schema

```json
{
  "_id": "ObjectId",
  "source": "String",
  "title": "String",
  "description": "String",
  "content": "String",
  "author": "String",
  "published_at": "Date",
  "url": "String",
  "keywords": ["String"],
  "ingested_at": "Date"
}
