package com.crawler.tests

import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.{BasicDBObject, MongoClient}

import collection.JavaConversions._

/**
  * Created by max on 29.12.16.
  */
object MongoTests {
  def main(args: Array[String]) {
    val collection = new MongoClient("localhost").getDatabase("Crawler").getCollection("posts", classOf[BasicDBObject])
    val distinct = collection.find().projection(new BasicDBObject("key", 1)).toArray
    println(distinct.length)
  }
}

object MongoUtils {
  def main(args: Array[String]): Unit = {
    new MongoClient("localhost")
      .getDatabase("SocialApp")
      .createCollection("Posts", new CreateCollectionOptions().capped(true).sizeInBytes(99999999999L))
  }
}
