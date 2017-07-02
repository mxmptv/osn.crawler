package com.crawler.osn.instagram

import akka.actor.{ActorSystem, Props}
import com.crawler.core.actors.WorkerActor
import com.crawler.core.balancers.{InitBalancer, Balancer}
import com.crawler.dao.MongoSaverInfo

import scalaj.http.Http

/**
  * Created by max on 02.12.16.
  */
object InstagramProfileTest {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("VkBalancer")
    val balancer = actorSystem.actorOf(Props[Balancer])

    actorSystem.actorOf(WorkerActor.props()).tell(InitBalancer(), balancer)

    implicit val appname = "testApp"

    balancer ! InstagramProfileTask(
      //      profileId = "boldasarini_make_up",
      profileId = 390955269L,
      saverInfo = MongoSaverInfo(host = "192.168.13.133", db = "test_db", collectionName = "instagram_profiles_test")
    )
  }
}


object InstagramSearchTest {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("VkBalancer")
    val balancer = actorSystem.actorOf(Props[Balancer])

    actorSystem.actorOf(WorkerActor.props()).tell(InitBalancer(), balancer)

    implicit val appname = "testApp"

    balancer ! InstagramSearchPostsTask(
      query = "spb",
      count = 1000000,
      saverInfo = MongoSaverInfo(host = "192.168.13.133", db = "test_db", collectionName = "instagram_spb_test_speed2")
    )
  }
}

object InstagramPostsTest {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("VkBalancer")
    val balancer = actorSystem.actorOf(Props[Balancer])

    actorSystem.actorOf(WorkerActor.props()).tell(InitBalancer(), balancer)

    implicit val appname = "testApp"

    balancer ! InstagramPostsTask(
      username = "boldasarini_make_up",
      //      count = 1000,
      saverInfo = MongoSaverInfo(host = "192.168.13.133", db = "test_db", collectionName = "instagram_posts_test")
    )
  }
}


object insttest {
  def main(args: Array[String]): Unit = {

    val username = "boldasarini_make_up"
    //    val response = Jsoup.connect(s"https://instagram.com/$username/?__a=1").ignoreContentType(true).execute().body()

    val response = Http(s"https://instagram.com/$username/?__a=1").header("User-Agent", "ScalaBOT 1.0").execute()
    println(response)

    /* wtf ??? */

  }
}