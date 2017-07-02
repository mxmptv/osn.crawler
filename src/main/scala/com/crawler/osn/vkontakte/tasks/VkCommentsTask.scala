package com.crawler.osn.vkontakte.tasks

import akka.actor.ActorRef
import com.mongodb.util.JSON
import com.mongodb.{BasicDBList, BasicDBObject}
import com.crawler.osn.common.{ResponseTask, SaveTask, VkontakteAccount, VkontakteTask}
import com.crawler.dao.{MemorySaverInfo, SaverInfo}

import scalaj.http.Http

/**
  * Created by vipmax on 31.10.16.
  */
case class VkCommentsTask(ownerId: String,
                          postId: String,
                          var count: Int = 1000000,
                          override val responseActor: AnyRef = null,
                          saverInfo: SaverInfo = MemorySaverInfo())(implicit app: String)
  extends VkontakteTask
    with ResponseTask
    with SaveTask {

  override def appname: String = app

  override def extract(account: VkontakteAccount) = {
    var end = false
    var offset = 0

    while(!end) {
      val request = Http("https://api.vk.com/method/wall.getComments?")
        .param("owner_id", ownerId.toString)
        .param("post_id", postId.toString)
        .param("count", "100")
        .param("need_likes", "1")
        .param("offset", offset.toString)
        .param("v", "5.8")
        .timeout(60 * 1000 * 10, 60 * 1000 * 10)

      val comments = JSON.parse(request.execute().body).asInstanceOf[BasicDBObject]
        .get("response").asInstanceOf[BasicDBObject]
        .get("items").asInstanceOf[BasicDBList].toArray()
        .map { case b:BasicDBObject =>
          b.append("key", s"${ownerId}_${postId}_${b.getString("from_id")}_${b.getString("id")}")
            .append("post_owner", ownerId)
            .append("post_id", postId)
        }

      offset += comments.length
      count -= comments.length

      if (comments.length < 100 || count <= 0) end = true

      save(comments)
//      response(responseActor, VkRepostsTaskResponse(this, comments))
    }
  }

  override def name: String = s"VkCommentsTask(item=$ownerId)"

}

