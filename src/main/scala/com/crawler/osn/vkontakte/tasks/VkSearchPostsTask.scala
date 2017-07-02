package com.crawler.osn.vkontakte.tasks

import akka.actor.ActorRef
import com.mongodb.util.JSON
import com.mongodb.{BasicDBList, BasicDBObject}
import com.crawler.osn.common._
import com.crawler.dao.{MemorySaverInfo, SaverInfo}
import org.joda.time.DateTime

import scalaj.http.Http

/**
  * Created by vipmax on 31.10.16.
  */

case class VkSearchPostsTaskDataResponse(task: VkSearchPostsTask, resultData: Array[BasicDBObject], params: Map[String, String])  extends TaskDataResponse

case class VkSearchPostsTask(query: String,
                             startTime: Long = DateTime.now().minusDays(1).getMillis / 1000,
                             endTime: Long = DateTime.now().getMillis / 1000,
                             override val responseActor: ActorRef = null,
                             saverInfo: SaverInfo = MemorySaverInfo()
                            )(implicit app: String)
  extends VkontakteTask
    with SaveTask
    with ResponseTask
    with StateTask {

  override def name: String = s"VkSearchPostsTask(query=$query, startTime=$startTime, endTime=$endTime)"

  override def appname: String = app

  val httpRequest = Http("https://api.vk.com/method/newsfeed.search")
    .param("q", query.toString)
    .param("count", "200")
    .param("start_time", startTime.toString)
    .param("end_time", endTime.toString)
    .param("v", "5.13")

  override def extract(account: VkontakteAccount) = {
    var end = false
    var startFrom = ""

    while(!end) {
      val json = exec(httpRequest.param("start_from", startFrom))
      val (posts, nextFrom, totalCount) = parse(json)
      startFrom = nextFrom

      logger.debug(s"Found ${posts.length} posts, totalCount=$totalCount with startfrom=$startFrom nextFrom=$nextFrom for $id"
      )

      if (nextFrom == "" ) end = true

      save(posts)
      response(VkSearchPostsTaskDataResponse(this.copy(), posts, httpRequest.params.toMap))
      onResult(posts)
    }
  }

  private def parse(json: String) = {
    val bObject = JSON.parse(json).asInstanceOf[BasicDBObject]
      .get("response").asInstanceOf[BasicDBObject]

    val totalCount = bObject.getInt("total_count")
    val nextFrom = bObject.getString("next_from")

    val posts = bObject
      .get("items").asInstanceOf[BasicDBList].toArray
      .map { case b: BasicDBObject => b
        .append("key", s"${b.getString("from_id")}_${b.getString("id")}")
        .append("search_query_params", httpRequest.params.mkString(", "))
        .append("totalCount", totalCount)
      }

    (posts, nextFrom, totalCount)
  }

  private def timeIsEnd(posts: Array[BasicDBObject]): Boolean = {
    val lastPostDate = posts.last.get("date").asInstanceOf[Int]
    startTime <= lastPostDate && lastPostDate <= endTime
  }
}

case class VkSearchPostsExtendedTask(params: Map[String, String],
                                     responseActor: ActorRef = null,
                                     saverInfo: SaverInfo)(implicit app: String) extends VkontakteTask {

  override def name: String = s"VkSearchPostsExtendedTask(params=$params)"

  override def appname: String = app

  val request = Http("https://api.vk.com/method/newsfeed.search")
    .params(params)
    .param("v", "5.8")
    .timeout(60 * 1000 * 10, 60 * 1000 * 10)

  override def extract(account: VkontakteAccount) = {

    val json = request.execute().body

    val posts = try {
      JSON.parse(json).asInstanceOf[BasicDBObject]
        .get("response").asInstanceOf[BasicDBObject]
        .get("items").asInstanceOf[BasicDBList].toArray
        .map { case b: BasicDBObject => b.append("key", s"${b.getString("from_id")}_${b.getString("id")}")
          .append("search_query_params", request.params.toMap)
        }
    } catch {
      case e: Exception =>
        logger.error(e)
        logger.error(json)
        Array[BasicDBObject]()
    }
  }

}

