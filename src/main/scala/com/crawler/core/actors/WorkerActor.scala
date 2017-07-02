package com.crawler.core.actors

import akka.actor.{Actor, ActorRef, Props}
import org.apache.log4j.Logger
import com.crawler.core.actors.WorkerActor.SimpleWorkerTaskRequest
import com.crawler.core.balancers.InitBalancer
import com.crawler.osn.common._
import com.crawler.dao._
import com.crawler.logger.{CrawlerLogger, CrawlerLoggerFactory}
import com.crawler.util.Util
import twitter4j.conf.ConfigurationBuilder
import twitter4j.{Twitter, TwitterFactory}

import scala.util.Random
/**
  * Created by vipmax on 14.11.16.
  */
object WorkerActor {
  def props(account: Account = null) = Props(new WorkerActor(account))

  case class SimpleWorkerTaskRequest(task: Task)
}

class WorkerActor(account: Account = null) extends Actor {
  val logger: CrawlerLogger = CrawlerLoggerFactory.logger("WorkerActor","core/workers")
  val network = buildNetwork(account)
  var balancer: ActorRef = _
  var proxies = Util.getHttpProxies()

  override def receive: Receive = {
    case task: Task =>
      logger.debug(s"task id=${task.id}  name=$task  type=${task.getClass} ")

      Util.injectDependencies(logger, balancer,task)

      try {
        task.run(network)
      } catch {
        case NeedRepeatTaskException(cause) =>
          task.attempt += 1
          logger.info(s"NeedRepeatTaskException $cause task.id=${task.id} task.attempt=${task.attempt}")
          Thread.sleep(10000)

//          if(cause.contains("IP")) {
//            task.proxy = proxies(Random.nextInt(proxies.length))
//            logger.debug(s"proxy changed for task.id=${task.id}")
//          }

          Util.uninjectDependencies(task)

          if(task.attempt < 5) balancer ! task
          else {
            logger.error(s"task.attempt >= 5 ${task.id}. Skiping task")
          }

        case e: Exception =>
          logger.error(e)
          logger.error(e.getStackTraceString)

      }

      Util.uninjectDependencies(task)

      balancer ! SimpleWorkerTaskRequest(task)


    case InitBalancer() =>
      logger.debug(s"Init balancer with sender=$sender")
      balancer = sender
      balancer ! SimpleWorkerTaskRequest(null)

    case _ =>
      throw new RuntimeException("World is burning!!")
  }




  private def buildNetwork(account: Account) = {
    account match {
      case a: TwitterAccount => buildTwitter(a)
      case a: VkontakteAccount => a
      case a: InstagramAccount => a
      case _ => null
    }
  }

  private def buildTwitter(twitterAccount: TwitterAccount): Twitter = {
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



