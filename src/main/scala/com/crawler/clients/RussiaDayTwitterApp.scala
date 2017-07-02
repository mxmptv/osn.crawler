package com.crawler.clients

import akka.actor.{ActorSystem, Props}
import com.crawler.core.runners.{CrawlerAgent, CrawlerClient, CrawlerConfig, CrawlerMaster}
import com.crawler.dao.{FileSaverInfo, MongoSaverInfo}
import com.crawler.osn.common.TaskDataResponse
import com.crawler.osn.twitter.tasks.TwitterSearchPostsTask
import com.crawler.util.Util
import com.mongodb.{BasicDBObject, CursorType, MongoClient}
import org.bson.BsonDocument
import twitter4j.Query

/**
  * Created by max on 11.05.17.
  */
object RussiaDayTwitterApp {
  implicit val name = "RussiaDayTwitterApp"


  def main(args: Array[String]) {
    CrawlerMaster.main(Array())
    CrawlerAgent.main(Array())

    val masterIp = if (args.length > 0) args(0) else Util.getCurrentIp()
    val myIp = if (args.length > 1) args(1) else Util.getCurrentIp()

    val clusterName = "crawler"
    val system = ActorSystem(clusterName,
      CrawlerConfig.getConfig(clusterName, masterIp, myIp, name)
    )
    system.actorOf(Props[RussiaDayTwitterApp], name)
    system.whenTerminated
  }

  class RussiaDayTwitterApp extends CrawlerClient {

    override def afterBalancerWakeUp() = {
//      val task = TwitterSearchPostsTask(
//        query = new Query().query("#ДеньРоссии"),
//        count = 100000,
//        saverInfo = MongoSaverInfo("192.168.13.133", name, s"TwitterPosts")
//      )
      val task = TwitterSearchPostsTask(
        query = new Query().query("#russia"),
        count = 100000,
        saverInfo = FileSaverInfo("russia.txt")
      )
      twitterBalancer ! task
    }

    override def handleTaskDataResponse(tr: TaskDataResponse) = tr match {
      case any: TaskDataResponse =>
        println(s"any response = ${any.getClass.getSimpleName}")
    }
  }
}
