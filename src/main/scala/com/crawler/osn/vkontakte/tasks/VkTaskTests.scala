package com.crawler.osn.vkontakte.tasks

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import org.escience.core.osn.vkontakte.tasks.VkProfileTask
import com.crawler.core.actors.WorkerActor
import com.crawler.core.balancers.{InitBalancer, Balancer, TwitterBalancer}
import com.crawler.dao._

/**
  * Created by vipmax on 22.11.16.
  */

object TestProfile {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("VkBalancer")
    val balancer = actorSystem.actorOf(Props[TwitterBalancer])

    actorSystem.actorOf(Props[WorkerActor]).tell(InitBalancer(), balancer)

    implicit val appname = "testApp"

    val vkGroupProfileTask = new VkProfileTask(
      profileIds = List("1", "2", "-1", "-2"),
      saverInfo = KafkaSaverInfo(endpoint = "192.168.13.133:9092", topic = "test")
    )
    balancer ! vkGroupProfileTask
  }
}


object TestRemoteProfile {
  def main(args: Array[String]) {
    val ip = "127.0.0.1"
    val akkaSystemName = "App"

    val config: String = s"""
      akka {
          actor {
            provider = "akka.remote.RemoteActorRefProvider"
          }
          remote {
            log-remote-lifecycle-events = off
            netty.tcp {
              hostname = "$ip"
              port = 0
            }
          }
          serializers {
            java = "akka.serialization.JavaSerializer"
          }
      }
    """
    val actorSystem = ActorSystem(akkaSystemName, ConfigFactory.parseString(config))
    val balancer = actorSystem.actorSelection("akka.tcp://VkBalancer@127.0.0.1:2551/user/balancer")

    implicit val appname = "testApp"

    val vkGroupProfileTask = VkProfileTask(
      profileIds = List("1", "2", "-1", "-2"),
      saverInfo = KafkaSaverInfo(endpoint = "192.168.13.133:9092", topic = "test")
    )

    balancer ! vkGroupProfileTask
  }
}

object TestFollowers {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("VkBalancer")
    val balancer = actorSystem.actorOf(Props[Balancer])

    actorSystem.actorOf(Props[WorkerActor]).tell(InitBalancer(), balancer)

    implicit val appname = "testApp"

    balancer ! new VkFollowersTask(
      profileId = "-1",
      saverInfo = MongoSaverInfo(host = "192.168.13.133", db = "test_db", collectionName = "test_relations")
    )
  }
}

object TestFollowersExtended {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("VkBalancer")
    val balancer = actorSystem.actorOf(Props[Balancer])

    actorSystem.actorOf(Props[WorkerActor]).tell(InitBalancer(), balancer)

    implicit val appname = "testApp"

//    balancer ! new VkFollowersExtendedTask(
//      profileId = "-1",
//      saverInfo = MongoSaverInfo2(endpoint = "192.168.13.133", db = "test_db", collection = "test_relations", collection2 = "test_users")
//    )
  }
}

object TestPosts {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("VkBalancer")
    val balancer = actorSystem.actorOf(Props[Balancer])

    actorSystem.actorOf(Props[WorkerActor]).tell(InitBalancer(), balancer)

    implicit val appname = "testApp"

    balancer ! new VkPostsTask(
      ownerId = "-1",
      saverInfo = MongoSaverInfo(host = "192.168.13.133", db = "test_db", collectionName = "test_posts")
    )
  }
}

object TestLikes {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("VkBalancer")
    val balancer = actorSystem.actorOf(Props[Balancer])

    actorSystem.actorOf(WorkerActor.props()).tell(InitBalancer(), balancer)

    implicit val appname = "testApp"

    balancer ! VkLikesTask(
      ownerId = "-32370614",
      itemId = "52196",
      saverInfo = MongoSaverInfo(host = "192.168.13.133", db = "test_db", collectionName = "test_likes")
    )
  }
}
object TestReposts {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("VkBalancer")
    val balancer = actorSystem.actorOf(Props[Balancer])

    actorSystem.actorOf(WorkerActor.props()).tell(InitBalancer(), balancer)

    implicit val appname = "testApp"

    balancer ! VkRepostsTask(
      ownerId = "-86529522",
      postId = "71509",
      saverInfo = MongoSaverInfo(host = "192.168.13.133", db = "test_db", collectionName = "test_reposts")
    )
  }
}
object TestComments {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("VkBalancer")
    val balancer = actorSystem.actorOf(Props[Balancer])

    actorSystem.actorOf(WorkerActor.props()).tell(InitBalancer(), balancer)

    implicit val appname = "testApp"

    balancer ! VkCommentsTask(
      ownerId = "-15755094",
      postId = "14101795",
      saverInfo = MongoSaverInfo(host = "192.168.13.133", db = "test_db", collectionName = "test_comments")
    )
  }
}

object TestSearchPosts {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("VkBalancer")
    val balancer = actorSystem.actorOf(Props[Balancer])

    actorSystem.actorOf(Props[WorkerActor]).tell(InitBalancer(), balancer)

    implicit val appname = "testApp"

    balancer ! new VkSearchPostsTask(
      query = "spb",
      saverInfo = MongoSaverInfo(host = "192.168.13.133", db = "test_db", collectionName = "test_posts_spb")
    )
  }
}


object PrichislenkoCrawler {
  def main(args: Array[String]) {

    val actorSystem = ActorSystem("VkBalancer")
    val balancer = actorSystem.actorOf(Props[Balancer])

    actorSystem.actorOf(Props[WorkerActor]).tell(InitBalancer(), balancer)

    implicit val appname = "PrichislenkoApp"

    val ids = Array(
      "-41240468",
      "-74058720",
      "-50305445",
      "-81526971",
      "-47214165",
      "-465",
      "-36286006",
      "-55821382",
      "-30525261",
      "-23611958",
      "-38119975",
      "-41538339",
      "-86218441"
    )

    ids.foreach{ id =>
      balancer ! new VkPostsTask(
        ownerId = id,
        saverInfo = MongoSaverInfo(host = "192.168.13.133", db = "Prichislenko", collectionName = "posts")
      )
    }
  }
}
