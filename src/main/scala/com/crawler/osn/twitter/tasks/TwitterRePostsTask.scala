package com.crawler.osn.twitter.tasks

import akka.actor.ActorRef
import com.mongodb.BasicDBObject
import com.crawler.osn.common.{SaveTask, TwitterTask}
import com.crawler.dao.SaverInfo
import twitter4j._

import scala.collection.JavaConversions._

case class TwitterRePostsTaskResponse(statusId: Long, reposts: List[BasicDBObject])

case class TwitterRePostsTask(statusId: Long,
                              responseActor: ActorRef = null,
                              saverInfo: SaverInfo)(implicit app: String)
  extends TwitterTask
    with SaveTask {

  override def appname: String = app

  override def run(network: AnyRef) {
    network match {
      case twitter: Twitter => extract(twitter)
      case _ => logger.debug("No TwitterTemplate object found")
    }
  }


  def extract(twitter: Twitter) {

    val statuses = twitter.tweets().getRetweets(statusId)

    logger.info(s"Saving ${statuses.length} retweets for $statusId statusId Limits = ${statuses.getRateLimitStatus}")
    val posts = TwitterTaskUtil.mapStatuses(statuses.toList)

    save(posts)
    Option(responseActor) match {
      case Some(actor) => actor ! TwitterRePostsTaskResponse(statusId, posts)
      case None => logger.debug(s"No response Actor for task $name")
    }

    logger.debug(s"Ended for $statusId. retweetsCount = ${posts.length}")
  }


  override def name: String = s"TwitterPostsTask(statusId=$statusId)"

  override def newRequestsCount() = 1
}
