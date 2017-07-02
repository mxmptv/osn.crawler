package com.crawler.osn.instagram

import com.mongodb.BasicDBObject
import com.mongodb.util.JSON
import com.crawler.osn.common.{InstagramTask, SaveTask, VkontakteTask}
import com.crawler.dao.SaverInfo
import org.jsoup.Jsoup

/**
  * Created by vipmax on 31.10.16.
  */
case class InstagramProfileTask(profileId: Any, saverInfo: SaverInfo)(implicit app: String)
  extends InstagramTask
    with SaveTask {

  override def appname: String = app

  override def run(network: AnyRef) {

    val json = profileId match {
      case username: String =>
        Jsoup.connect(s"https://instagram.com/$username/?__a=1").ignoreContentType(true).execute().body()
      case id: Long =>
        logger.debug(s"Needs to get username by id=$id")
        "not implemented yet"
    }

    val userInfo = JSON.parse(json).asInstanceOf[BasicDBObject].get("user").asInstanceOf[BasicDBObject]
    userInfo.append("key", userInfo.getString("id"))

    save(userInfo)
  }

  override def name: String = s"InstagramProfileTask(userId=$profileId)"

}

