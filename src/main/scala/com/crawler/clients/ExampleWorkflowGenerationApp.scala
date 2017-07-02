package com.crawler.clients

import akka.actor.{ActorSystem, Props}
import org.escience.core.osn.vkontakte.tasks.VkProfileTask
import com.crawler.osn.common.TaskDataResponse
import com.crawler.osn.vkontakte.tasks._
import com.crawler.core.runners.{CrawlerAgent, CrawlerClient, CrawlerConfig, CrawlerMaster}
import com.crawler.dao.MongoSaverInfo
import com.crawler.util.Util

/**
  * Created by max on 11.05.17.
  */
object ExampleWorkflowGenerationApp {
  implicit val name = "ExampleWorkflowGenerationApp"

  val seeds = List("-68471405")

  def main(args: Array[String]) {
    CrawlerMaster.main(Array())
    CrawlerAgent.main(Array())

    val masterIp = if (args.length > 0) args(0) else Util.getCurrentIp()
    val myIp = if (args.length > 1) args(1) else Util.getCurrentIp()

    val clusterName = "crawler"
    val system = ActorSystem(clusterName,
      CrawlerConfig.getConfig(clusterName, masterIp, myIp, name)
    )
    system.actorOf(Props[ExampleWorkflowGenerationApp], name)
    system.whenTerminated
  }

  class ExampleWorkflowGenerationApp extends CrawlerClient {

    override def afterBalancerWakeUp() {
      seeds.foreach { seed =>
        val vkFollowersTask = VkFollowersTask(
          profileId = seed, count = 100,
          responseActor = selfActorPath,
          saverInfo = MongoSaverInfo("192.168.13.133", name, s"followers")
        )
        vkFollowersTask.account = null
        vkFollowersTask.onResult = (data) => {
          println("lambda " + data)
        }

        send(vkFollowersTask)
      }
    }

    override def handleTaskDataResponse(tr: TaskDataResponse) = tr match {
      case any: TaskDataResponse =>
        println(s"any response = ${any.getClass.getSimpleName}")
    }
  }
}
