package com.crawler.osn.vkontakte.tasks

import akka.actor.ActorRef
import com.mongodb.util.JSON
import com.mongodb.{BasicDBList, BasicDBObject}
import com.crawler.osn.common._
import com.crawler.dao.{MemorySaverInfo, SaverInfo}

import scalaj.http.Http

/**
  * Created by vipmax on 31.10.16.
  */
case class VkRepostsTaskDataResponse(task: VkRepostsTask, resultData: Array[BasicDBObject]) extends TaskDataResponse

case class VkRepostsTask(ownerId: String,
                         postId: String,
                         count: Int = 1000,
                         override val responseActor: AnyRef = null,
                         saverInfo: SaverInfo = MemorySaverInfo()
                        )(implicit app: String)
  extends VkontakteTask
    with ResponseTask
    with SaveTask
    with StateTask
    with FrequencyLimitedTask {

  override def appname: String = app

  override def extract(account: VkontakteAccount) {
    var end = false
    var offset = state.getOrElse("offset", 0).asInstanceOf[Int]

    while(!end) {
      val request = Http("https://api.vk.com/method/wall.getReposts")
        .param("owner_id", ownerId.toString)
        .param("post_id", postId.toString)
        .param("count", "1000")
        .param("offset", offset.toString)
        .param("v", "5.8")
        .timeout(60 * 1000 * 10, 60 * 1000 * 10)

      val json = exec(request)
      logger.trace(json)

      if(json.contains("too many requests from your IP")){
        throw NeedRepeatTaskException("too many requests from your IP")
      }

      val parsed = JSON.parse(json).asInstanceOf[BasicDBObject]

      val reposts = parsed
        .get("response").asInstanceOf[BasicDBObject]
        .get("items").asInstanceOf[BasicDBList].toArray()
        .map{ case b:BasicDBObject =>
          b.append("key", s"${b.getString("from_id")}_${b.getString("id")}")
        }

      offset += reposts.length

      save(reposts)
      response(responseActor, VkRepostsTaskDataResponse(this.copy(), reposts))
      saveState(Map("offset" -> offset))

      if (reposts.length < 10 || offset >= count) end = true
    }
  }

  override def name: String = s"VkRepostsTask(ownerId=$ownerId, postId=$postId)"

}


