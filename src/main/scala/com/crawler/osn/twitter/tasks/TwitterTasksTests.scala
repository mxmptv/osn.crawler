package com.crawler.osn.twitter.tasks

import akka.actor.{ActorSystem, Props}
import com.crawler.core.actors.twitter.TwitterSequentialTypedWorkerActor
import com.crawler.core.balancers.{InitBalancer, TwitterBalancer}
import com.crawler.dao.MongoSaverInfo
import com.crawler.util.Util
import twitter4j.Query

/**
  * Created by vipmax on 29.11.16.
  */
object TwitterPostsTaskTest {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("TwitterBalancer")
    val balancer = actorSystem.actorOf(Props[TwitterBalancer])

    val accounts = Util.getTwitterAccounts().take(1)

    accounts foreach { account =>
      actorSystem.actorOf(TwitterSequentialTypedWorkerActor.props(account)).tell(InitBalancer(), balancer)
    }

    implicit val appname = "testApp"

    balancer ! TwitterPostsTask(
//      profileId = "@djvipmax_",
      profileId = "@Rogozin",
      saverInfo = MongoSaverInfo(host = "192.168.13.133", db = "test_db", collectionName = "test_twitter")
    )
  }
}
object TwitterRePostsTaskTest {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("TwitterBalancer")
    val balancer = actorSystem.actorOf(Props[TwitterBalancer])

    val accounts = Util.getTwitterAccounts().take(1)

    accounts foreach { account =>
      actorSystem.actorOf(TwitterSequentialTypedWorkerActor.props(account)).tell(InitBalancer(), balancer)
    }

    implicit val appname = "testApp"

    balancer ! TwitterRePostsTask(
      statusId = 806123544878915584L,
      saverInfo = MongoSaverInfo(host = "192.168.13.133", db = "test_db", collectionName = "test_retweets")
    )
  }
}

object TwitterRePostersTaskTest {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("TwitterBalancer")
    val balancer = actorSystem.actorOf(Props[TwitterBalancer])

    val accounts = Util.getTwitterAccounts().take(1)

    accounts foreach { account =>
      actorSystem.actorOf(TwitterSequentialTypedWorkerActor.props(account)).tell(InitBalancer(), balancer)
    }

    implicit val appname = "testApp"

    balancer ! TwitterRePostersTask(
      statusId = 806123544878915584L,
      count = 10000,
      saverInfo = MongoSaverInfo(host = "192.168.13.133", db = "test_db", collectionName = "test_reposters")
    )
  }
}


object TwitterFollowersTaskTest {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("TwitterBalancer")
    val balancer = actorSystem.actorOf(Props[TwitterBalancer])

    val accounts = Util.getTwitterAccounts().take(1)

    accounts foreach { account =>
      actorSystem.actorOf(TwitterSequentialTypedWorkerActor.props(account)).tell(InitBalancer(), balancer)
    }

    implicit val appname = "testApp"

    balancer ! TwitterFollowersTask(
//      profileId = "@djvipmax_",
      profileId = "@Rogozin",
      saverInfo = MongoSaverInfo(host = "192.168.13.133", db = "test_db", collectionName = "test_twitter_followers")
    )
  }
}

object TwitterProfileTest {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("TwitterBalancer")
    val balancer = actorSystem.actorOf(Props[TwitterBalancer])

    val accounts = Util.getTwitterAccounts().take(1)

    accounts foreach { account =>
      actorSystem.actorOf(TwitterSequentialTypedWorkerActor.props(account)).tell(InitBalancer(), balancer)
    }

    implicit val appname = "testApp"

    balancer ! TwitterProfileTask(
      //      profileId = "@djvipmax_",
      profileId = "@Rogozin",
      saverInfo = MongoSaverInfo(host = "192.168.13.133", db = "test_db", collectionName = "test_user_profiles")
    )
  }
}

object TwitterSearchPostsTest {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("TwitterBalancer")
    val balancer = actorSystem.actorOf(Props[TwitterBalancer])

    val accounts = Util.getTwitterAccounts().take(1)

    accounts foreach { account =>
      actorSystem.actorOf(TwitterSequentialTypedWorkerActor.props(account)).tell(InitBalancer(), balancer)
    }

    implicit val appname = "testApp"

    val query = new Query()
    query.setQuery("warriors")

    balancer ! TwitterSearchPostsTask(
      query = query,
      count = 140,
      saverInfo = MongoSaverInfo(host = "192.168.13.133", db = "test_db", collectionName = "test_warriors_tweets")
    )
  }
}
