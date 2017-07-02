package com.crawler.dao

/**
  * Created by max on 02.07.17.
  */

trait SaverInfo

case class MemorySaverInfo() extends SaverInfo

case class FileSaverInfo(filePath: String) extends SaverInfo

case class MongoSaverInfo(host: String, db: String, collectionName: String) extends SaverInfo

case class MongoSaverInfo2(endpoint: String, db: String, collection: String, collection2: String) extends SaverInfo

case class KafkaSaverInfo(endpoint: String, topic: String) extends SaverInfo

case class KafkaUniqueSaverInfo(kafkaEndpoint: String, redisEndpoint: String, topic: String) extends SaverInfo

case class RedisSaverInfo(redisEndpoint: String, prefix: String) extends SaverInfo

case class RDBSSaverInfo(endpoint: String, db: String, tableName: String) extends SaverInfo

