package com.crawler.osn.vkontakte.tasks

import akka.actor.ActorRef
import com.mongodb.util.JSON
import com.mongodb.{BasicDBList, BasicDBObject}
import com.crawler.osn.common.Filters.{CountFilter, TimeFilter}
import com.crawler.osn.common._
import com.crawler.dao.{MemorySaverInfo, SaverInfo}
import org.joda.time.DateTime
import twitter4j.{ResponseList, Status}

import scalaj.http.Http

/**
  * Created by vipmax on 31.10.16.
  */

case class VkPostsTaskDataResponse(task: VkPostsTask, resultData: Array[BasicDBObject]) extends TaskDataResponse

case class VkPostsTask(ownerId:String,
                       countFilter: CountFilter = CountFilter(),
                       timeFilter: TimeFilter = TimeFilter(),
                       override val responseActor: AnyRef = null,
                       saverInfo: SaverInfo = MemorySaverInfo()
                      )(implicit app: String)
  extends VkontakteTask
    with SaveTask
    with ResponseTask {

  override def appname: String = app

  override def extract(account: VkontakteAccount) = {
    var end = false
    var offset = 0
    val maxPostsCount = 100

    while(!end) {
      val httpRequest = Http("https://api.vk.com/method/wall.get")
        .param("owner_id", ownerId.toString)
        .param("count", maxPostsCount.toString)
        .param("offset", offset.toString)
        .param("v", "5.8")

      val json = exec(httpRequest)

      val posts = parse(json)
      offset += posts.length

      save(posts)
      response(VkPostsTaskDataResponse(this.copy(), posts))
      onResult(posts)

      if (posts.length < maxPostsCount || isOutOfTime(posts.last.getInt("date"), timeFilter)) end = true
    }
  }

  def isOutOfTime(lastPostTimestamp: Int, timeFilter: TimeFilter): Boolean = {
    val maxFromTime = timeFilter.fromTime.getMillis / 1000
    val isOutOfTime = lastPostTimestamp < maxFromTime
    if(isOutOfTime) {
      val lastPostTime = new DateTime(lastPostTimestamp.toLong * 1000).toString("yyyy-MM-dd HH:mm")
      val maxFromTime = timeFilter.fromTime.toString("yyyy-MM-dd HH:mm")
      logger.debug(s"isOutOfTime (lastPostTime=$lastPostTime < maxFromTime$maxFromTime)")
    }

    isOutOfTime
  }

//  def isEnd(lastPostTimestamp: Int, currentPostsCount: Int) = {
//    val isOutOfTime = lastPostTimestamp < timeFilter.fromTime.getMillis / 1000
//    if(isOutOfTime) logger.debug(s"isOutOfTime ($lastPostTimestamp < ${timeFilter.fromTime.getMillis})")
//    if(isOutOfTime) logger.debug(s"isOutOfTime (${new DateTime(lastPostTimestamp*1000)} < ${timeFilter.fromTime})")
//    val isEnough = currentPostsCount > countFilter.maxCount
//    if(isEnough) logger.debug(s"isEnough ($currentPostsCount < ${countFilter.maxCount})")
//
//    isOutOfTime ||  isEnough
//  }


  private def parse(json: String) = {
    JSON.parse(json).asInstanceOf[BasicDBObject]
      .get("response").asInstanceOf[BasicDBObject]
      .get("items").asInstanceOf[BasicDBList].toArray
      .map { case b: BasicDBObject =>
        b.append("key", s"${b.getString("from_id")}_${b.getString("id")}")
      }
  }

  override def name: String = s"VkPostsTask(ownerId=$ownerId)"
}

