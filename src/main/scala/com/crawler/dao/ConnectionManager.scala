package com.crawler.dao

import scala.collection.mutable

/**
  * Created by max on 06.12.16.
  */

object ConnectionManager {
  private val mongoPool = mutable.HashMap[String, MongoSaver]()
  private val filePool = mutable.HashMap[String, FileSaver]()
  private val rdbsPool = mutable.HashMap[String, RDBSSaver]()
  private val kafkaPool = mutable.HashMap[String, KafkaSaver]()
  private val kafkaUniquePool = mutable.HashMap[String, KafkaUniqueSaver]()
  private val redisPool = mutable.HashMap[String, RedisSaver]()

  def getSaver(saverInfo: SaverInfo): Saver = synchronized {
    saverInfo match {
      case info: FileSaverInfo =>
        val key = info.filePath
        if (!filePool.contains(key)) filePool(key) = new FileSaver(info)
        filePool(key)

      case info: RDBSSaverInfo =>
        val key = s"${info.endpoint} ${info.db} ${info.tableName}"
        if (!rdbsPool.contains(key)) rdbsPool(key) = new RDBSSaver(info)
        rdbsPool(key)

      case info: MongoSaverInfo =>
        val key = s"${info.host} ${info.db} ${info.collectionName}"
        if (!mongoPool.contains(key)) mongoPool(key) = new MongoSaver(info)
        mongoPool(key)

      case info: KafkaSaverInfo =>
        val key = s"${info.endpoint} ${info.topic}"
        if (!kafkaPool.contains(key)) kafkaPool(key) = new KafkaSaver(info)
        kafkaPool(key)

      case info: KafkaUniqueSaverInfo =>
        val key = s"${info.kafkaEndpoint} ${info.redisEndpoint} ${info.topic}"
        if (!kafkaUniquePool.contains(key)) kafkaUniquePool(key) = new KafkaUniqueSaver(info)
        kafkaUniquePool(key)

      case info: RedisSaverInfo =>
        val key = s"${info.redisEndpoint} ${info.prefix}"
        if (!redisPool.contains(key)) redisPool(key) = new RedisSaver(info)
        redisPool(key)

      case _ => null
    }
  }
}