package com.crawler.osn.twitter.tasks

import com.mongodb.BasicDBObject
import com.mongodb.util.JSON
import twitter4j.{Status, TwitterObjectFactory}

/**
  * Created by vipmax on 30.11.16.
  */

/* Look at https://dev.twitter.com/rest/public/rate-limits */
object TwitterTaskUtil {
  def getAllSlots() = {
    val slots = Map(
      "TwitterFollowersTask" -> 15,
      "TwitterPostsTask" -> 900,
      "TwitterRePostsTask" -> 75,
      "TwitterRePostersTask" -> 75,
      "TwitterSearchPostsTask" -> 180,
      "TwitterProfileTask" -> 900
    )
    println(slots)
    slots
  }
  def getAllTasks() = {
    val tasks = Set(
      "TwitterFollowersTask",
      "TwitterProfileTask",
      "TwitterSearchPostsTask",
      "TwitterPostsTask",
      "TwitterRePostsTask",
      "TwitterRePostersTask"
    )
    println(tasks)
    tasks
  }

  def mapStatuses(statuses: List[Status]) = {
    statuses.map { status =>
      val json = TwitterObjectFactory.getRawJSON(status)
      val basicDBObject = JSON.parse(json).asInstanceOf[BasicDBObject]
      basicDBObject.append("key", s"${basicDBObject.getString("id")}")
    }
  }
}
