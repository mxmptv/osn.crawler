package com.crawler.core.actors.twitter


import akka.actor.{Actor, ActorRef, Props}
import akka.routing.RoundRobinPool
import org.apache.log4j.Logger
import com.crawler.core.actors.twitter.TwitterSequentialTypedWorkerActor.TwitterTypedWorkerTaskRequest
import com.crawler.core.actors.twitter.TwitterTaskExecutorActor.TaskResult
import com.crawler.core.balancers.{InitBalancer, UpdateSlots}
import com.crawler.osn.common.{TwitterAccount, TwitterTask}
import com.crawler.osn.twitter.tasks.TwitterTaskUtil
import com.crawler.dao.{KafkaUniqueSaver, KafkaUniqueSaverInfo, _}
import com.crawler.logger.CrawlerLoggerFactory
import com.crawler.util.Util
import twitter4j.conf.ConfigurationBuilder
import twitter4j.{Twitter, TwitterException, TwitterFactory}

import scala.collection.mutable
import scala.concurrent.duration.{FiniteDuration, _}

/**
  * Created by max on 08.12.16.
  */

object TwitterParrallelTypedWorkerActor {
  def props(account: TwitterAccount, requestsMaxPerTask: Map[String, Int] = TwitterTaskUtil.getAllSlots()) =
    Props(new TwitterParrallelTypedWorkerActor(account, requestsMaxPerTask))

}

class TwitterParrallelTypedWorkerActor(account: TwitterAccount,
                                       requestsMaxPerTask: Map[String, Int]) extends Actor {
  private val logger = CrawlerLoggerFactory.logger("TwitterParrallelTypedWorkerActor","core/workers")

  /* tasktype and requests available */
  private val slots = mutable.Map[String, Int](requestsMaxPerTask.toList: _*)
  /* after blockedTime all slots will be as initial */
  private val blockedTime: FiniteDuration = 15 minutes

  private var twitter: Twitter = {
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
  private var balancer: ActorRef = _

  private val twitterTaskExecutorActorPool = context.system.actorOf(
    RoundRobinPool(10).props(TwitterTaskExecutorActor.props(twitter))
  )

  override def receive: Receive = {
    case task: TwitterTask =>
      logger.debug(s"task = $task ${task.getClass}")

      Util.injectDependencies(logger,balancer, task)

      twitterTaskExecutorActorPool ! task


    case TaskResult(task, e) =>
      logger.info(s"TaskResult = $task ${task.getClass}")
      Util.uninjectDependencies(task)

      e match {
        case e: TwitterException if e.getRateLimitStatus.getRemaining <= 0 =>
          slots(task.taskType()) = 0
          logger.error(s"RateLimit Exception!!! ${e.getRateLimitStatus} Trying once more for task $task!!!")
          Thread.sleep(1000)
          balancer ! task

        case e: Exception =>
          logger.error(s"Ignoring exception for task $task ${e.getMessage.split("\n").mkString(" ")}")

        case null => logger.error(s"Task is ended $task ")
      }

      /* slots updating */
      slots(task.taskType()) -= task.newRequestsCount()

      logger.debug(s"slots = $slots")

      /* asking new task */
      val freeSlots = getFreeSlots()
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

  private def getFreeSlots() = {
    slots.filter { case (_, requestsLeft) => requestsLeft > 0 }.keys.toSet
  }

}




