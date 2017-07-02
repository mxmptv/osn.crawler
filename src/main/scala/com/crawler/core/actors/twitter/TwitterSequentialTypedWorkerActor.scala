package com.crawler.core.actors.twitter

import akka.actor.{Actor, ActorRef, Props}
import org.apache.log4j.Logger
import com.crawler.core.actors.twitter.TwitterSequentialTypedWorkerActor.TwitterTypedWorkerTaskRequest
import com.crawler.core.balancers.{InitBalancer, UpdateSlots}
import com.crawler.osn.common.{Task, TwitterAccount, TwitterTask}
import com.crawler.osn.twitter.tasks.TwitterTaskUtil
import com.crawler.dao.{KafkaUniqueSaver, KafkaUniqueSaverInfo, _}
import com.crawler.logger.CrawlerLoggerFactory
import com.crawler.util.Util
import twitter4j.conf.ConfigurationBuilder
import twitter4j.{Twitter, TwitterException, TwitterFactory}

import scala.collection.mutable
import scala.concurrent.duration.{FiniteDuration, _}

/**
  * Created by djvip on 13.08.2016.
  */
object TwitterSequentialTypedWorkerActor {

  def props(account: TwitterAccount, requestsMaxPerTask: Map[String, Int] = TwitterTaskUtil.getAllSlots()) =
    Props(new TwitterSequentialTypedWorkerActor(account, requestsMaxPerTask))

  case class TwitterTypedWorkerTaskRequest(
    freeSlots: Set[String] ,
    previousTask: Task = null
  )
}


class TwitterSequentialTypedWorkerActor(account: TwitterAccount,
                                        requestsMaxPerTask: Map[String, Int]
                                       ) extends Actor {
  val logger = CrawlerLoggerFactory.logger("TwitterSequentialTypedWorkerActor","core/workers")
  /* tasktype and requests available */
  val slots = mutable.Map[String, Int](requestsMaxPerTask.toList: _*)
  val blockedTime: FiniteDuration = 15 minutes

  var twitter: Twitter = {
    val cb = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
      .setJSONStoreEnabled(true)
      .setOAuthConsumerKey(account.key)
      .setOAuthConsumerSecret(account.secret)
      .setOAuthAccessToken(account.token)
      .setOAuthAccessTokenSecret(account.tokenSecret)
    val twitter = new TwitterFactory(cb.build()).getInstance()
    twitter
  }
  var balancer: ActorRef = _

  override def receive: Receive = {
    case task: TwitterTask =>
      logger.info(s"task = $task ${task.getClass}")

      Util.injectDependencies(logger, balancer, task)
      /* running task */
      try {
        task.run(twitter)

        /* slots updating */
        slots(task.taskType()) -= task.newRequestsCount()

      } catch {
         case e: TwitterException if e.getRateLimitStatus.getRemaining <= 0 =>
          slots(task.taskType()) = 0
          logger.error(s"RateLimit!!! ${e.getRateLimitStatus} Trying once more for task $task!!!")
          Thread.sleep(1000)
          Util.uninjectDependencies(task)
          balancer ! task

        case e: Exception =>
          logger.error(s"Ignoring exception for task $task $e")
      }

      logger.debug(s"slots = $slots")

      Util.uninjectDependencies(task)


      /* asking new task */
      val freeSlots = slots.filter { case (_, requestsLeft) => requestsLeft > 0 }.keys.toSet
      balancer ! TwitterTypedWorkerTaskRequest(freeSlots, task)


    case InitBalancer() =>
      logger.info(s"Init balancer with sender=$sender")
      balancer = sender
      balancer ! TwitterTypedWorkerTaskRequest(requestsMaxPerTask.keys.toSet)

      /* updating slots each $blockedTime seconds */
      context.system.scheduler.scheduleOnce(
        blockedTime, self, UpdateSlots()
      )(context.dispatcher)

    case UpdateSlots() =>
      slots.keys.foreach { taskType =>
        slots(taskType) = requestsMaxPerTask(taskType)
      }
      logger.info(s"all slots updated $slots")

      balancer ! TwitterTypedWorkerTaskRequest(requestsMaxPerTask.keys.toSet)

      /* updating slots each $blockedTime seconds */
      context.system.scheduler.scheduleOnce(
        blockedTime, self, UpdateSlots()
      )(context.dispatcher)

    case _ =>
      throw new RuntimeException("Unknown message type!")
  }

}



