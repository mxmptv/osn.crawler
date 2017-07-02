package com.crawler.osn.twitter.tasks

import akka.actor.ActorRef
import com.mongodb.BasicDBObject
import com.crawler.osn.common.Filters.{CountFilter, TimeFilter}
import com.crawler.osn.common._
import com.crawler.dao.{MemorySaver, MemorySaverInfo, SaverInfo}
import twitter4j._

import scala.collection.JavaConversions._

/**
  * Created by vipmax on 29.11.16.
  */
case class TwitterPostsTaskDataResponse(task: TwitterPostsTask, resultData: Array[BasicDBObject]) extends TaskDataResponse

case class TwitterPostsTask(profileId: Any,
                            timeFilter: TimeFilter = TimeFilter(),
                            countFilter: CountFilter = CountFilter(3200),
                            override val responseActor: ActorRef = null,
                            saverInfo: SaverInfo = MemorySaverInfo()
                           )(implicit app: String)
  extends TwitterTask with StateTask with SaveTask with ResponseTask {

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
    var end = false
    /* read state */
    var localOffset = offset
    var statusesCount = 0
    val maxPostsCount = 100

    while (!end) {

      val paging = new Paging(localOffset.toInt, maxPostsCount)
      logger.debug(paging)

      val statuses = profileId match {
        case id: String =>
          twitter.timelines().getUserTimeline(id, paging)
        case id: Long =>
          twitter.timelines().getUserTimeline(id, paging)
        case id: Int =>
          twitter.timelines().getUserTimeline(id, paging)
      }
      _newRequestsCount += 1
      statusesCount += statuses.length

      logger.info(s"Saving ${statuses.length} tweets for $profileId Limits = ${statuses.getRateLimitStatus}")
      val posts = TwitterTaskUtil.mapStatuses(statuses.toList).toArray

      if (statuses.isEmpty || isEnd(statuses, statusesCount) ) {
        logger.debug(s"Ended for $profileId. statusesCount = $statusesCount")
        end = true
      }

      localOffset += 1
      saveState(Map("offset" -> localOffset))

      save(posts)
      response(TwitterPostsTaskDataResponse(this, posts))
    }
  }


  def isEnd(statuses: ResponseList[Status], statusesCount: Int) = {
    val isNotInTime = statuses.last.getCreatedAt.getTime < timeFilter.fromTime.getMillis
    val isEnough = statusesCount > countFilter.maxCount
    isNotInTime ||  isEnough
  }

  override def name: String = s"TwitterPostsTask(profileId=$profileId)"

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
