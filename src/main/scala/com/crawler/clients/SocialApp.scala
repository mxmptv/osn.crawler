package com.crawler.clients

import akka.actor.{ActorSystem, Props}
import com.crawler.core.runners.{CrawlerAgent, CrawlerClient, CrawlerConfig, CrawlerMaster}
import com.crawler.dao.KafkaUniqueSaverInfo
import com.crawler.osn.common.TaskDataResponse
import com.crawler.osn.instagram.InstagramNewGeoPostsSearchTask
import com.crawler.util.Util
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import scala.concurrent.duration._

/**
  * Created by max on 11.06.17.
  */
object SocialApp {
  implicit val name = "SocialApp"

  def main(args: Array[String]) {
    CrawlerMaster.main(Array())
    CrawlerAgent.main(Array())

    Thread.sleep(5000)

    val masterIp = if (args.length > 0) args(0) else Util.getCurrentIp()
    val myIp = if (args.length > 1) args(1) else Util.getCurrentIp()

    val clusterName = "crawler"
    val system = ActorSystem(clusterName,
      CrawlerConfig.getConfig(clusterName, masterIp, myIp, name)
    )
    system.actorOf(Props[SocialApp], name)
    system.whenTerminated
  }

  class SocialApp extends CrawlerClient {
    override def afterBalancerWakeUp() {
      context.system.scheduler.schedule(
        0 seconds, 10 seconds, self, "instagram"
      )(context.dispatcher)
    }

    override def receiveMassage(massage: Any): Unit = massage match {
      case "instagram" =>
        TopicsExtractor.get().foreach { topic =>
          val task = InstagramNewGeoPostsSearchTask(
            query = topic,
            saverInfo = KafkaUniqueSaverInfo("localhost:9092", "localhost", "posts")
          )
          send(task)
        }
    }
    override def handleTaskDataResponse(tr: TaskDataResponse) = tr match {
      case any: TaskDataResponse =>
        println(s"any response = ${any.getClass.getSimpleName}")
    }
  }
}

object TopicsExtractor{
  import scala.collection.JavaConversions._

  val pool = new JedisPool(new JedisPoolConfig(), "localhost")
  val jedis = pool.getResource

  val key = "topics"

  def get() = {
    val topics = jedis.smembers(key).toSet
    println("Found topics", topics)
    topics
  }
}
