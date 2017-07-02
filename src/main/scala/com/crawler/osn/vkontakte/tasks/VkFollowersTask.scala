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
case class VkFollowersTaskDataResponse(task: VkFollowersTask, resultData: Array[BasicDBObject]) extends TaskDataResponse

case class VkFollowersTask(profileId: String,
                           count: Int = 100000000,
                           saverInfo: SaverInfo = MemorySaverInfo(),
                           override val responseActor: AnyRef = null
                              )(implicit app: String)
  extends VkontakteTask
    with SaveTask
    with ResponseTask {

  val methodName = if (profileId.toLong > 0)  "friends.get" else "groups.getMembers"
  val paramProfileKey = if (profileId.toLong > 0)  "user_id" else "group_id"
  val paramProfileValue = if (profileId.toLong > 0)  profileId else profileId.substring(1)
  val parseKey = if (profileId.toLong > 0) "items" else "users"

  override def appname: String = app

  override def extract(account: VkontakteAccount) {
    var end = false
    var offset = 0
    val maxCount = 1000

    while(!end) {
      val httpRequest = Http(s"https://api.vk.com/method/$methodName")
        .param(paramProfileKey, paramProfileValue)
        .param("offset", offset.toString)
        .param("count", maxCount.toString)
        .param("v", "5.8")

      val json = exec(httpRequest)

      if(json.contains("error")){
        logger.error(s"error $json")
        return
      }

      val followers = parse(json)
      offset += followers.length

      if (followers.length < maxCount || offset >= count) end = true

//      save(followers)
      onResult(followers)
      response(VkFollowersTaskDataResponse(this.copy(), followers))
    }
  }

  private def parse(json: String) = {
    JSON.parse(json).asInstanceOf[BasicDBObject]
      .get("response").asInstanceOf[BasicDBObject]
      .get(parseKey).asInstanceOf[BasicDBList].toArray.map(_.asInstanceOf[Int])
      .map { f =>
        new BasicDBObject()
          .append("key", s"${profileId}_$f")
          .append("profile", profileId)
          .append("follower", f.toString)
      }
  }

  override def name: String = s"VkFollowersTask(userId=$profileId)"

}
//
//case class VkFollowersExtendedTask(profileId:String, saverInfo: SaverInfo)(implicit app: String) extends VkontakteTask {
//
//  val methodName = if (profileId.toLong > 0)  "friends.get" else "groups.getMembers"
//  val paramProfileKey = if (profileId.toLong > 0)  "user_id" else "group_id"
//  val paramProfileValue = if (profileId.toLong > 0)  profileId else profileId.substring(1)
//  val parseKey = if (profileId.toLong > 0) "items" else "users"
//  val userFields =  "photo_id, verified, sex, bdate, city, country, home_town, " +
//    "has_photo, photo_50, photo_100, photo_200_orig, photo_200, " +
//    "photo_400_orig, photo_max, photo_max_orig, online, lists, " +
//    "domain, has_mobile, contacts, site, education, universities, " +
//    "schools, status, last_seen, followers_count,  occupation, nickname, " +
//    "relatives, relation, personal, connections, exports, wall_comments, " +
//    "activities, interests, music, movies, tv, books, games, about, quotes, " +
//    "can_post, can_see_all_posts, can_see_audio, can_write_private_message, " +
//    "can_send_friend_request, is_favorite, is_hidden_from_feed, timezone, " +
//    "screen_name, maiden_name, crop_photo, is_friend, friend_status, career," +
//    " military, blacklisted, blacklisted_by_me"
//  val groupFields =  "sex, bdate, city, country, photo_50, photo_100, photo_200_orig, " +
//    "photo_200, photo_400_orig, photo_max, photo_max_orig, online, online_mobile," +
//    " lists, domain, has_mobile, contacts, connections, site, education, universities," +
//    " schools, can_post, can_see_all_posts, can_see_audio, can_write_private_message," +
//    " status, last_seen, relation, relatives"
//  val fields = if (profileId.toLong > 0) userFields else groupFields
//
//  override def appname: String = app
//
//  override def extract(account: VkontakteAccount) = {
//
//    var end = false
//    var offset = 0
//    val maxCount = 1000
//
//    while(!end) {
//      val res = Http(s"https://api.vk.com/method/$methodName")
//        .param(paramProfileKey, paramProfileValue)
//        .param("offset", offset.toString)
//        .param("count", maxCount.toString)
//        .param("v", "5.8")
//        .param("fields", fields)
//        .timeout(60 * 1000 * 10, 60 * 1000 * 10)
//        .execute().body
//
//      val friends = try {
//        JSON.parse(res).asInstanceOf[BasicDBObject]
//          .get("response").asInstanceOf[BasicDBObject]
//          .get(parseKey).asInstanceOf[BasicDBList].toArray
//      } catch {case e: Exception =>
//        logger.error(res)
//        Array[BasicDBObject]()
//      }
//
//
//      if (friends.length < maxCount) end = true
//      offset += friends.length
//
//
//      Option(saver) match {
//        case Some(s) =>
//          val data = friends.map { case bdo: BasicDBObject => new BasicDBObject()
//              .append("key", s"${profileId}_${bdo.getString("id")}")
//              .append("profile", profileId)
//              .append("follower", bdo.getString("id"))
//          }
//          s.save(data.toList)
//        case None => logger.debug(s"No saver for task $name")
////      }
////      Option(saver2) match {
////        case Some(s) =>
////          val data = friends.map { case bdo: BasicDBObject =>
////            bdo.append("key", bdo.getInt("id"))
////          }
////          s.save(data.toList)
////        case None => logger.debug(s"No saver for task $name")
////      }
//
//    }
//  }
//
//  override def name: String = s"VkUserFollowersExtendedTask(userId=$profileId)"
//}


