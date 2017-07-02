package com.crawler.osn.youtube

import java.util.Date

import com.crawler.dao.{MemorySaverInfo, SaverInfo}
import com.crawler.osn.common.{SaveTask, StateTask, YoutubeTask}
import com.crawler.util.Util
import com.mongodb.{BasicDBList, BasicDBObject}
import com.mongodb.util.JSON

import scalaj.http.Http

/**
  * Created by max on 01.07.17.
  */
case class YoutubeTopsTask(country: String, saverInfo: SaverInfo = MemorySaverInfo())(implicit app: String)
  extends YoutubeTask
    with SaveTask
    with StateTask {

  /** task name */
  override def name: String = s"YoutubeTopsTask($country)"

  /** task name */
  override def appname: String = app

  /** Method for run task. Must include network logic to different OSNs */
  override def run(network: AnyRef): Unit = {

    var offset = 0
    var pageToken = ""
    val nowDate = new Date()


    val request = Http("https://content.googleapis.com/youtube/v3/videos")
      .param("regionCode", country)
      .param("chart", "mostPopular")
      .param("part", "snippet,contentDetails,statistics")
      .param("maxResults", "50")
      .param("pageToken", pageToken)
      .timeout(60 * 1000 * 10, 60 * 1000 * 10)

    val json = exec(request)

    val result = JSON.parse(json).asInstanceOf[BasicDBObject]

    val videos = result
      .get("items").asInstanceOf[BasicDBList].toArray()
      .map { case b:BasicDBObject =>
        offset += 1
        b.append("key", s"${b.getString("id")}_${country}_${nowDate}")
         .append("county", country)
         .append("update_time", nowDate)
         .append("top_position", offset)
      }.toIterable

    save(videos)
  }
}

