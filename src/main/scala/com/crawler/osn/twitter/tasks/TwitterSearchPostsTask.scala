package com.crawler.osn.twitter.tasks

import java.util

import akka.actor.ActorRef
import com.mongodb.BasicDBObject
import com.crawler.osn.common._
import com.crawler.osn.vkontakte.tasks.VkPostsTask
import com.crawler.dao.{MemorySaverInfo, SaverInfo}
import twitter4j._

import scala.collection.JavaConversions._

/**
  * Created by vipmax on 29.11.16.
  */
case class TwitterSearchPostsTaskFinalDataResponse(task: TwitterSearchPostsTask, resultData: Array[BasicDBObject]) extends TaskDataResponse
case class TwitterSearchPostsTaskDataResponse(task: TwitterSearchPostsTask, resultData: Array[BasicDBObject]) extends TaskDataResponse

case class TwitterSearchPostsTask(query: Query,
                                  var count: Int = -1,
                                  saverInfo: SaverInfo = MemorySaverInfo(),
                                  override val responseActor: AnyRef = null
                                 )(implicit app: String)
  extends TwitterTask
    with SaveTask
    with StateTask
    with ResponseTask {

  /* state */
  var offset = 1L
  var _newRequestsCount = 0

  override def appname: String = app

  override def run(network: AnyRef) {
    network match {
      case twitter: Twitter => extract(twitter)
      case _ => logger.debug("No TwitterTemplate object found")
    }
  }

  def extract(twitter: Twitter) {
    var searchResult = twitter.search().search(query.count(100))
    _newRequestsCount += 1

    val tweets = searchResult.getTweets
    tweets.foreach { t => logger.trace(t.getCreatedAt) }
    count -= tweets.length
    logger.trace(List.fill(28)("#").mkString)
    saveTweets(tweets)

    while (searchResult.hasNext) {
      searchResult = twitter.search().search(searchResult.nextQuery())
      _newRequestsCount += 1
      val tweets = searchResult.getTweets
      tweets.foreach { t => logger.trace(t.getCreatedAt) }
      count -= tweets.length

      saveTweets(tweets)
      logger.trace(List.fill(28)("#").mkString)
    }

//    response(TwitterSearchPostsTaskFinalDataResponse(this, Array()))
  }

  private def saveTweets(tweets: util.List[Status]) = {
    val data = TwitterTaskUtil.mapStatuses(tweets.toList).toArray
    save(data)
    logger.info(s"Saved ${data.length} tweets")
//    response(TwitterSearchPostsTaskDataResponse(this, data))
  }

  override def name: String = s"TwitterSearchPostsTask(query=${query.getQuery})"

  override def saveState(stateParams: Map[String, Any]) {
    logger.debug(s"Saving state. stateParams=$stateParams")
    val offset = stateParams.getOrElse("offset", -1).toString.toLong
    this.offset = offset
  }

  override def newRequestsCount() = {
    val returned = _newRequestsCount
    _newRequestsCount = 0
    returned
  }
}
