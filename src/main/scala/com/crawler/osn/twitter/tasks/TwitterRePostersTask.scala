package com.crawler.osn.twitter.tasks

import akka.actor.ActorRef
import com.mongodb.BasicDBObject
import com.crawler.osn.common.{SaveTask, StateTask, TwitterTask}
import com.crawler.dao.SaverInfo
import twitter4j._

case class TwitterRePostersTaskResponse(statusId:Long, reposters: Array[BasicDBObject])

case class TwitterRePostersTask(statusId:Long,
                                var count: Int = 100,
                                responseActor: ActorRef = null,
                                saverInfo: SaverInfo)(implicit app: String)
  extends TwitterTask
    with StateTask
    with SaveTask {

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

    while (!end) {

      val statuses = twitter.tweets().getRetweeterIds(statusId, localOffset)

      _newRequestsCount += 1
      count -= statuses.getIDs.length

      logger.info(s"Saving ${statuses.getIDs.length} tweets for $statusId Limits = ${statuses.getRateLimitStatus}")

      val data = statuses.getIDs().map{ id => new BasicDBObject()
        .append("key", s"${statusId}_$id")
        .append("statusId", statusId)
        .append("reposterId", id)
      }

      save(data)

      if(!statuses.hasNext || count <= 0) {
        logger.debug(s"Ended for $statusId. statusesCount = $count")
        end = true
      }

      localOffset = statuses.getNextCursor
      saveState(Map("offset" -> localOffset))
    }
  }

  override def name: String = s"TwitterRePostersTask(statusId=$statusId)"

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
