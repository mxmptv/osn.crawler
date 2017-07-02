package com.crawler.core.runners

import akka.actor.{ActorSelection, ActorSystem, Props}
import akka.cluster.ClusterEvent.{MemberEvent, MemberRemoved, MemberUp, UnreachableMember}
import akka.cluster.Member
import akka.serialization.Serialization
import com.crawler.core.actors.WorkerActor
import com.crawler.core.actors.twitter.TwitterParrallelTypedWorkerActor
import com.crawler.core.balancers.{Balancer, TwitterBalancer}
import com.crawler.osn.common.{Account, TwitterAccount}
import com.crawler.core.runners.CrawlerAgent.{AddWorkerRequest, RemoveWorkerRequest, WorkerUp}
import com.crawler.util.Util

import scala.collection.mutable

/**
  * Created by max on 05.05.17.
  */

object CrawlerAgent {
  case class AddWorkerRequest(workerType:String, account:Account)
  case class RemoveWorkerRequest(workerType:String)

  case class WorkerUp(workerPath:String)
  case class WorkerRemoved(workerPath:String)

  val role = "CrawlerAgent"

  def main(args: Array[String]) {
    val masterIp = if (args.length > 0) args(0) else Util.getCurrentIp()
    val myIp = if (args.length > 1) args(1) else Util.getCurrentIp()

    val clusterName = "crawler"
    val system = ActorSystem(
      clusterName,
      CrawlerConfig.getConfig(clusterName = clusterName, masterIp = masterIp, myIp = myIp, role = role)
    )
    system.actorOf(Props[CrawlerAgent], role)
    system.whenTerminated
  }
}

class CrawlerAgent extends CrawlerActor {
  var crawlerMaster: ActorSelection = _
  var workersCount = 0
  val workers = mutable.ArrayBuffer[String]()

  override def receive = {
    case AddWorkerRequest(workerType, account) =>
      log.info(s"AddWorker $workerType from $sender")
      createWorker(workerType, account)

    case RemoveWorkerRequest(w) =>
      log.info(s"RemoveWorker $w from $sender")
      removeWorker(w)

    case MemberUp(member) =>
      log.info(s"MemberUp: $member roles = ${member.roles.mkString}")
      if (member.hasRole(CrawlerMaster.role)) onMasterUp(member)

    case MemberRemoved(member, prevStatus) =>
      log.info(s"MemberRemoved: node $member is removed after $prevStatus")
      if (member.hasRole(CrawlerMaster.role)) onMasterDown(member)

    case event: MemberEvent =>
      log.info(s"event: $event")
  }

  private def onMasterUp(member: Member) = {
    val masterAdress = member.address.toString
    log.info(s"CrawlerMaster is up on $masterAdress")
    crawlerMaster = context.actorSelection(s"$masterAdress/user/${CrawlerMaster.role}")
  }
  private def onMasterDown(member: Member) = {
    System.exit(-1)
  }

  private def removeWorker(w: String) = workers -= w
  private def createWorker(workerType: String, account: Account) = {
    val (balancer, wo) = workerType match {
      case "simple" => makeSimpleWorker(account)
      case "twitter" => makeTwitterWorker(account)
    }

    workersCount += 1
    val worker = Serialization.serializedActorPath(wo)
    workers += worker

    balancer ! WorkerUp(worker)
    crawlerMaster ! WorkerUp(worker)
  }


  private def makeTwitterWorker(account: Account) = {
    val balancer = context.actorSelection(s"${sender.path.toString}/${TwitterBalancer.name}")
    val worker = context.actorOf(TwitterParrallelTypedWorkerActor.props(
      account.asInstanceOf[TwitterAccount]), "twitter-worker-" + workersCount
    )
    (balancer, worker)
  }
  private def makeSimpleWorker(account: Account) = {
    val balancer = context.actorSelection(s"${sender.path.toString}/${Balancer.name}")
    val worker = context.actorOf(WorkerActor.props(account), "simple-worker-" + workersCount)
    (balancer, worker)
  }
}
