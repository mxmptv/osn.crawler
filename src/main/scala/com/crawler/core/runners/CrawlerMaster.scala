package com.crawler.core.runners

import akka.actor.{ActorSelection, ActorSystem, Props, RootActorPath}
import akka.cluster.ClusterEvent._
import akka.cluster.Member
import akka.serialization.Serialization
import com.crawler.core.balancers._
import com.crawler.core.runners.CrawlerAgent.{AddWorkerRequest, WorkerRemoved, WorkerUp}
import com.crawler.core.web.CrawlerWebServer
import com.crawler.util.Util

import scala.collection.mutable

/**
  * Created by max on 27.04.17.
  */
object CrawlerMaster {
  val role = "CrawlerMaster"

  def main(args: Array[String]) {
    val masterIp = if (args.length > 0) args(0) else Util.getCurrentIp()

    val clusterName = "crawler"
    val actorSystem = ActorSystem(
      clusterName,
      CrawlerConfig.getConfig(clusterName, masterIp, masterIp, role)
    )
    actorSystem.actorOf(Props[CrawlerMaster], role)
    actorSystem.whenTerminated
  }
}

class CrawlerMaster extends CrawlerActor {
  val balancer = context.actorOf(Props[Balancer], Balancer.name)
  val twitterBalancer = context.actorOf(Props[TwitterBalancer], TwitterBalancer.name)

  val crawlerAgents = mutable.Map[ActorSelection, mutable.Map[String, Any]]()
  val crawlerClients = mutable.Set[ActorSelection]()

  val crawlerWebServer = new CrawlerWebServer(this)
  crawlerWebServer.start()

  override def receive: Receive = {
    case state: CurrentClusterState =>
      log.info("Current members: {}", state.members.mkString(", "))

    case MemberUp(member) =>
      log.info(s"MemberUp: $member roles = ${member.roles.mkString}")

      if (CrawlerConfig.isCrawlerAgent(member))  onAgentUp(member)
      if (CrawlerConfig.isCrawlerClient(member)) onClientUp(member)

    case MemberRemoved(member, prevStatus) =>
      log.info(s"MemberRemoved: node $member is removed after $prevStatus")

      if (CrawlerConfig.isCrawlerAgent(member))  onAgentDown(member)
      if (CrawlerConfig.isCrawlerClient(member)) onClientDown(member)

    case WorkerUp(worker) =>
      log.info(s"CrawlerMaster WorkerUp: $worker on $sender")
      onWorkerAdded(worker)

    case WorkerRemoved(worker) =>
      log.info(s"CrawlerMaster WorkerRemoved: $worker on $sender")

    case event: MemberEvent =>
      log.info(s"event: $event")
  }

  private def onWorkerAdded(worker: String) = {
    val agent = Serialization.serializedActorPath(sender)
    val agentActorSelection = context.actorSelection(agent.split("#")(0))
    crawlerAgents(agentActorSelection)("workers").asInstanceOf[mutable.Set[String]] += worker
    log.info("Crawler agents info: " + crawlerAgents)
  }

  private def onClientUp(member: Member) = {
    val clientName = CrawlerConfig.parseClientRole(member.roles.head)
    val actorSelection = context.actorSelection(RootActorPath(member.address) / "user" / clientName)
    crawlerClients += actorSelection
    log.info(s"CrawlerClient is up on ${member.address.toString} clientName=$clientName")
  }
  private def onClientDown(member: Member) = {
    val clientName = CrawlerConfig.parseClientRole(member.roles.head)
    crawlerClients -= context.actorSelection(RootActorPath(member.address) / "user" / clientName)
    balancer ! KillApp(clientName)
    log.info(s"CrawlerClient is down on ${member.address.toString} clientName=$clientName")
  }

  private def onAgentUp(member: Member) = {
    log.info(s"CrawlerAgent is up on ${member.address.toString}")
    val crawlerAgent = context.actorSelection(RootActorPath(member.address) / "user" / CrawlerAgent.role)
    crawlerAgents(crawlerAgent) = mutable.Map(
      "status" -> "up",
      "workers" -> mutable.Set(),
      "adress" -> crawlerAgent
    )

    /** init workers first time */
    initWorkers(crawlerAgent)
  }
  private def onAgentDown(member: Member) = {
    log.info(s"CrawlerAgent is down on ${member.address.toString}")
    val crawlerAgent = context.actorSelection(RootActorPath(member.address) / "user" / CrawlerAgent.role)
    crawlerAgents(crawlerAgent)("status") = "down"
  }


  private def initWorkers(crawlerAgent: ActorSelection) = {
    Util.getVkAccounts().take(10).foreach { account =>
      crawlerAgent ! AddWorkerRequest("simple", account)
    }

    Util.getTwitterAccounts().take(10).foreach { account =>
      crawlerAgent ! AddWorkerRequest("twitter", account)
    }
  }
}




