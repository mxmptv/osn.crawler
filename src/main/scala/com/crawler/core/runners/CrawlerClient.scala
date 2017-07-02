package com.crawler.core.runners

import akka.actor.{ActorSelection, ActorSystem, Props}
import akka.cluster.ClusterEvent.{MemberEvent, MemberRemoved, MemberUp, UnreachableMember}
import akka.serialization.Serialization
import com.crawler.core.balancers.{Balancer, TwitterBalancer}
import com.crawler.osn.common.{Task, TaskDataResponse, TwitterTask, VkontakteAccount}
import com.crawler.osn.vkontakte.tasks.{VkFollowersTask, VkFollowersTaskDataResponse}

/**
  * Created by max on 05.05.17.
  */


object CrawlerClient {
  implicit val name = "CrawlerClient"
  val role = "CrawlerClient"
}

abstract class CrawlerClient extends CrawlerActor {

  var simpleBalancer: ActorSelection = _
  var twitterBalancer: ActorSelection = _
  val selfActorPath: ActorSelection = context.actorSelection(Serialization.serializedActorPath(self))
  var tasksCounter = 0

  override def receive: Receive = {
    case MemberUp(member) =>
      log.debug(s"MemberUp: $member roles = ${member.roles.mkString}")

      if (member.hasRole(CrawlerMaster.role)) {
        val masterAdress = member.address.toString
        log.info(s"Master is up on $masterAdress")

        simpleBalancer = context.actorSelection(s"$masterAdress/user/${CrawlerMaster.role}/${Balancer.name}")
        twitterBalancer = context.actorSelection(s"$masterAdress/user/${CrawlerMaster.role}/${TwitterBalancer.name}")

        afterBalancerWakeUp()
      }

    case UnreachableMember(member) =>
      log.debug(s"UnreachableMember: node $member is unreachable")

    case MemberRemoved(member, prevStatus) =>
      log.debug(s"MemberRemoved: node $member is removed after $prevStatus")

    case event: MemberEvent =>
      log.debug(s"event: $event")

    case tr:TaskDataResponse =>
      log.debug(tr.task.toString)
      handleTaskDataResponse(tr)

    case massage =>
      receiveMassage(massage)
  }

  def send(task: Task) {
    val balancer = task match {
      case t: TwitterTask => twitterBalancer
      case _ => simpleBalancer
    }

    balancer ! task
    tasksCounter += 1
    log.debug(s"tasksCounter=$tasksCounter")
  }

  def handleTaskDataResponse(tr: TaskDataResponse)
  def receiveMassage(massage: Any) = {}
  def afterBalancerWakeUp()

}