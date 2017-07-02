package org.escience.core.osn.vkontakte.tasks

import com.mongodb.util.JSON
import com.mongodb.{BasicDBList, BasicDBObject, DBObject}
import com.crawler.osn.common._
import com.crawler.osn.vkontakte.tasks.VkFollowersTask
import com.crawler.dao.{MemorySaverInfo, SaverInfo}

import scalaj.http.{Http, HttpRequest}

/**
  * Created by vipmax on 31.10.16.
  */
case class VkProfileTaskDataResponse(task: VkProfileTask, resultData: Array[BasicDBObject]) extends TaskDataResponse

case class VkProfileTask(profileIds: List[String],
                         saverInfo: SaverInfo = MemorySaverInfo()
                        )(implicit app: String)
  extends VkontakteTask
    with SaveTask
    with ResponseTask {

  override def appname: String = app

  def extract(account: VkontakteAccount) = {
    val users = profileIds.map(_.toLong).filter(_ > 0).mkString(",")
    val groups = profileIds.map(_.toLong).filter(_ < 0).map(-_).mkString(",")

    users.grouped(1000).foreach{ gusers =>
      val json = exec(usersRequest(gusers))

      val profiles = parse(json)
      logger.debug(s"Got ${profiles.length} user profiles")
      save(profiles)
    }

    groups.grouped(1000).foreach { ggroups =>
      val json = exec(groupsRequest(ggroups))
      logger.debug(json)

      val profiles = parse(json).map { bdo: BasicDBObject => bdo.append("key", s"-${bdo.getInt("id")}") }
      logger.debug(s"Got ${profiles.length} group profiles")

      save(profiles)
    }
  }

  private def usersRequest(gusers: String) = {
    Http(s"https://api.vk.com/method/users.get")
      .param("user_ids", gusers)
      .param("fields", userFields)
      .param("v", "5.8")
  }

  private def groupsRequest(ggroups: String) = {
    Http(s"https://api.vk.com/method/groups.getById")
      .param("group_ids", ggroups)
      .param("fields", group_fields)
      .param("v", "5.8")
  }

  def parse(json: String): Array[BasicDBObject] = {
    try {
      JSON.parse(json)
        .asInstanceOf[DBObject]
        .get("response").asInstanceOf[BasicDBList]
        .toArray().map { case bdo: BasicDBObject =>
          bdo.append("key", s"${bdo.getInt("id")}")
        }
    } catch {case e: Exception =>
      logger.error(json)
      Array[BasicDBObject]()
    }
  }

  override def name: String = s"VkProfileTask(profileIds=${profileIds.length})"


  val group_fields = "city, country, place, description, wiki_page, members_count, " +
    "counters, start_date, finish_date, can_post, can_see_all_posts, activity," +
    " status, contacts, links, fixed_post, verified, site,ban_info,counters"

  val userFields = "photo_id, verified, sex, bdate, city, country, home_town, " +
    "has_photo, photo_50, photo_100, photo_200_orig, photo_200, " +
    "photo_400_orig, photo_max, photo_max_orig, online, lists, " +
    "domain, has_mobile, contacts, site, education, universities, " +
    "schools, status, last_seen, followers_count,  occupation, nickname, " +
    "relatives, relation, personal, connections, exports, wall_comments, " +
    "activities, interests, music, movies, tv, books, games, about, quotes, " +
    "can_post, can_see_all_posts, can_see_audio, can_write_private_message, " +
    "can_send_friend_request, is_favorite, is_hidden_from_feed, timezone, " +
    "screen_name, maiden_name, crop_photo, is_friend, friend_status, career," +
    " military, blacklisted, blacklisted_by_me,counters"

}

