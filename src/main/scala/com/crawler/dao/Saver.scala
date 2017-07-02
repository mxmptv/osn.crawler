package com.crawler.dao

import java.io.FileWriter
import java.sql.DriverManager
import java.util.Date

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.{ReplaceOneModel, UpdateOptions}
import com.mongodb.{BasicDBList, BasicDBObject, MongoClient, MongoWriteException}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.Logger
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by vipmax on 18.11.16.
  */

trait Saver {
  val logger = Logger.getLogger(this.getClass)

  def saveOne(one: BasicDBObject)

  def saveMany(many: Iterable[BasicDBObject]) = many.foreach(saveOne)

}

case class MemorySaver() extends Saver {
  val savedData = mutable.LinkedHashSet[BasicDBObject]()

  override def saveOne(one: BasicDBObject) = synchronized {
    logger.trace(s"Needs to save BasicDBObject")
    savedData += one
  }

  override def saveMany(many: Iterable[BasicDBObject]) = synchronized {
    logger.trace(s"Needs to save ${many.size} BasicDBObjects")
    savedData ++= many
  }
}

case class FileSaver(path: String) extends Saver {
  val fw = new FileWriter(path, true)

  def this(fileSaverInfo: FileSaverInfo) {
    this(fileSaverInfo.filePath)
  }

  override def saveOne(one: BasicDBObject) = synchronized {
    logger.trace(s"Needs to save BasicDBObject")
    fw.write(one.asInstanceOf[BasicDBObject].toJson + "\n")
    fw.flush()
  }

  override def saveMany(many: Iterable[BasicDBObject]) = synchronized {
    logger.trace(s"Needs to save ${many.size} BasicDBObjects")
    fw.write(many.mkString("\n") + "\n")
    fw.flush()
  }
}

case class KafkaSaver(var kafkaEndpoint: String, topic: String) extends Saver {

  val props = Map[String, Object]("bootstrap.servers" -> kafkaEndpoint)
  val producer = new KafkaProducer[String, String](props, new StringSerializer, new StringSerializer)

  def this(kafkaSaverInfo: KafkaSaverInfo) {
    this(kafkaSaverInfo.endpoint, kafkaSaverInfo.topic)
  }

  override def saveOne(one: BasicDBObject) {
    logger.debug(s"Needs to save BasicDBObject data")
    producer.send(new ProducerRecord[String, String](topic, one.toJson))
  }
}

case class KafkaUniqueSaver(kafkaEndpoint: String, redisEndpoint: String, kafkaTopic: String) extends Saver {
  def this(kusi: KafkaUniqueSaverInfo) {
    this(kusi.kafkaEndpoint, kusi.redisEndpoint, kusi.topic)
  }

  val kafkaProps = Map[String, Object]("bootstrap.servers" -> kafkaEndpoint)
  val kafkaProducer = new KafkaProducer[String, String](kafkaProps, new StringSerializer, new StringSerializer)

  val redisPool = new JedisPool(new JedisPoolConfig(), redisEndpoint)
  val jedis = redisPool.getResource

  override def saveOne(d: BasicDBObject): Unit = {
    val key = s"$kafkaTopic-${d.getString("key", java.util.UUID.randomUUID.toString.substring(0, 20))}"
    val value = d.toJson
    logger.debug(s"Needs to save BasicDBObject data $value")

    val rsps: Long = try {
      jedis.setnx(key, "")
    } catch {
      case e: Exception => 1L
    }
    rsps match {
      case r if r <= 0 =>
        logger.debug(s"Key $key already saved to kafka")
        try {
          jedis.expire(key, 60)
        } catch {
          case e: Exception => 0L
        }

      case r if r > 0 =>
        logger.debug(s"Needs  save $key to kafka")
        kafkaProducer.send(new ProducerRecord[String, String](kafkaTopic, value))
    }
  }

}

case class RedisSaver(redisEndpoint: String, collection: String, updateValue: Boolean = true) extends Saver {
  def this(rsi: RedisSaverInfo) {
    this(rsi.redisEndpoint, rsi.prefix)
  }

  val pool = new JedisPool(new JedisPoolConfig(), redisEndpoint)

  override def saveOne(d: BasicDBObject) {
    val value = d.toJson

    val key = s"$collection-${d.getString("key", java.util.UUID.randomUUID.toString.substring(0, 20))}"
    logger.debug(s"Needs to save BasicDBObject with $key and data $value")

    val jedis = pool.getResource
    val rsps = jedis.setnx(key, value)

    if (rsps <= 0) {
      if (updateValue) {
        jedis.set(key, value)
        logger.debug(s"Key $key updated in redis")
      }
    }
    else {
      logger.debug(s"Key $key saved")
    }
  }
}

case class MongoSaver(host: String, db: String, collectionName: String) extends Saver {
  val collection = new MongoClient(host).getDatabase(db).getCollection(collectionName, classOf[BasicDBObject])

  def this(mongoSaverInfo: MongoSaverInfo) {
    this(mongoSaverInfo.host, mongoSaverInfo.db, mongoSaverInfo.collectionName)
  }

  private def update(collection: MongoCollection[BasicDBObject], key: String, dbo: BasicDBObject) = {
    val k = new BasicDBObject("_id", key)
    collection.updateOne(k, new BasicDBObject("$set", dbo), new UpdateOptions().upsert(true))
  }

  override def saveOne(d: BasicDBObject) = {
    val key = d.getString("key", java.util.UUID.randomUUID.toString.substring(0, 20))
    logger.trace(s"Needs to save BasicDBObject with key=$key and data=${d.toJson}")
    val updateResult = update(collection, key, d)
    logger.trace(updateResult)
  }

  override def saveMany(many: Iterable[BasicDBObject]): Unit = {
    logger.trace(s"Needs to save bulk data=${many.size}")

    val writes = many.map { case bdo: BasicDBObject =>
      val key = bdo.getString("key", java.util.UUID.randomUUID.toString.substring(0, 20))
      new ReplaceOneModel[BasicDBObject](new BasicDBObject("_id", key), bdo, new UpdateOptions().upsert(true))
    }
    val bulkWriteResult = collection.bulkWrite(writes.toList)
    logger.trace(bulkWriteResult)
  }
}


case class RDBSSaver(endpoint: String, db: String, tablename: String) extends Saver {

  def this(rDBSSaverInfo: RDBSSaverInfo) {
    this(rDBSSaverInfo.endpoint, rDBSSaverInfo.db, rDBSSaverInfo.tableName)
  }

  val connection = DriverManager.getConnection(endpoint)

  override def saveOne(one: BasicDBObject) {

    //    one.keys.foreach(println)
    //    println(List.fill(100)("*").mkString)

    val sql = s"INSERT INTO $tablename VALUES (${(1 to one.size).map(e => "?").mkString(", ")})"
    val statement = connection.prepareStatement(sql)

    one.toMap.zipWithIndex.foreach { case ((key, value), index: Int) =>
      val i = index + 1
      value match {
        case v: Boolean => statement.setBoolean(i, v)
        case v: String => statement.setString(i, v)
        case v: Int => statement.setInt(i, v)
        case v: Long => statement.setLong(i, v)
        case v: Double => statement.setDouble(i, v)
        case v: Float => statement.setFloat(i, v)
        case v: java.util.Date => statement.setLong(i, v.getTime / 1000)
        case v: java.sql.Date => statement.setDate(i, v)
        case v: Array[Byte] => statement.setBytes(i, v)
      }
    }
    statement.execute()
  }
}

