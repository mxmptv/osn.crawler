package com.crawler.osn.youtube

import com.crawler.dao.{MemorySaver, RDBSSaverInfo}
import com.crawler.logger.CrawlerLoggerFactory
import com.crawler.osn.common.YoutubeAccount
import com.crawler.util.Util

/**
  * Created by max on 01.07.17.
  */
object YoutubeTopsTaskTests {

  def main(args: Array[String]): Unit = {
    val key = "AIzaSyAh5vdbszGH-KIY_kyTw9-nfYNa9d-BEM8"
    val logger = CrawlerLoggerFactory.logger("YoutubeTopsTaskTests","tasks/tests")

    val url = "jdbc:mysql://localhost:3306/youtube_tops?user=root&password=mysql&useSSL=false&useUnicode=true&characterEncoding=UTF-8"

    val task = YoutubeTopsTask(country = "RU", RDBSSaverInfo(url,"youtube_tops","test"))(app = "testApp")
    task.account = YoutubeAccount(key)
    task.dataSchema = List("id$","medium.url")
    Util.injectDependencies(logger, null, task)

    task.run(null)

  }
}
