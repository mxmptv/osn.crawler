package com.crawler.core.actors

import akka.actor.{Actor, ActorRef, Props}
import org.apache.log4j.Logger
import com.crawler.core.actors.twitter.TwitterSequentialTypedWorkerActor.TwitterTypedWorkerTaskRequest
import com.crawler.core.balancers.InitBalancer
import com.crawler.osn.common.{TwitterAccount, TwitterTask}
import com.crawler.osn.twitter.tasks.TwitterTaskUtil
import com.crawler.dao.{KafkaUniqueSaver, KafkaUniqueSaverInfo, _}
import com.crawler.logger.CrawlerLoggerFactory
import com.crawler.util.Util
import twitter4j.conf.ConfigurationBuilder
import twitter4j.{Twitter, TwitterFactory}

/**
  * Created by vipmax on 29.11.16.
  */
object TwitterSimpleWorkerActor {
  def props(twitterAccount: TwitterAccount) = Props(new TwitterSimpleWorkerActor(twitterAccount))
}

class TwitterSimpleWorkerActor(twitterAccount: TwitterAccount) extends Actor {
  val logger = CrawlerLoggerFactory.logger("TwitterSimpleWorkerActor","core/workers")
  var balancer: ActorRef = _
  var twitter: Twitter = buildTwitter(twitterAccount)


  override def receive: Receive = {
    case task: TwitterTask =>
      logger.debug(s"task = $task ${task.getClass}")

      Util.injectDependencies(logger,balancer, task)

      task.run(twitter)
      balancer ! TwitterTypedWorkerTaskRequest(TwitterTaskUtil.getAllTasks(), previousTask = task)

    case InitBalancer() =>
      logger.debug(s"Init balancer with sender=$sender")
      balancer = sender
      balancer ! TwitterTypedWorkerTaskRequest(TwitterTaskUtil.getAllTasks())

    case _ =>
      throw new RuntimeException("World is burning!!")
  }

  def buildTwitter(twitterAccount: TwitterAccount): Twitter = {
    val cb = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
      .setJSONStoreEnabled(true)
      .setOAuthConsumerKey(twitterAccount.key)
      .setOAuthConsumerSecret(twitterAccount.secret)
      .setOAuthAccessToken(twitterAccount.token)
      .setOAuthAccessTokenSecret(twitterAccount.tokenSecret)
    val twitter = new TwitterFactory(cb.build()).getInstance()
    logger.debug(twitter)
    twitter
  }
}


