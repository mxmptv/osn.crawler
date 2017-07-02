package com.crawler.osn.vkontakte.tasks

import com.mongodb.util.JSON
import com.mongodb.{BasicDBList, BasicDBObject}
import com.crawler.osn.common._
import com.crawler.dao.{MemorySaverInfo, SaverInfo}

import scalaj.http.Http

/**
  * Created by vipmax on 31.10.16.
  */
case class VkLikesTaskResponse(task: VkLikesTask, resultData: Array[BasicDBObject]) extends TaskDataResponse

case class VkLikesTask(itemType: String = "post",
                       ownerId: String,
                       itemId: String,
                       var count: Int = 1000,
                       saverInfo: SaverInfo = MemorySaverInfo(),
                       override val responseActor: AnyRef = null
                      )(implicit app: String)
  extends VkontakteTask
  with ResponseTask
  with SaveTask
  with FrequencyLimitedTask {

  override def appname: String = app

  override def extract(account: VkontakteAccount) = {
    var end = false
    var offset = 0

    while(!end) {
      val req = Http("https://api.vk.com/method/likes.getList")
        .param("type", itemType.toString)
        .param("owner_id", ownerId.toString)
        .param("item_id", itemId.toString)
        .param("count", "1000")
        .param("offset", offset.toString)
        .param("v", "5.8")
        .timeout(60 * 1000 * 10, 60 * 1000 * 10)

      val res = exec(req)
      logger.debug(res,account)

      val likes = JSON.parse(res).asInstanceOf[BasicDBObject]
        .get("response").asInstanceOf[BasicDBObject]
        .get("items").asInstanceOf[BasicDBList].toArray()
        .map(e => e.asInstanceOf[Int])
        .map{likerId => new BasicDBObject()
          .append("key", s"${itemType}_${itemId}_${ownerId}_${likerId}")
          .append("itemType", itemType)
          .append("itemId", itemId.toLong)
          .append("ownerId", ownerId.toLong)
          .append("likerId", likerId.toLong)
        }

      offset += likes.length
      count -= likes.length

      if (likes.length < 10 || count <= 0) end = true

      save(likes)
      response(VkLikesTaskResponse(this.copy(), likes))
    }
  }

  override def name: String = s"VkLikeTask(item=$ownerId)"

}

