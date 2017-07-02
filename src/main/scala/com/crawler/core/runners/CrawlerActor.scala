package com.crawler.core.runners

import akka.actor.{Actor, ActorLogging}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberEvent, UnreachableMember}

/**
  * Created by max on 05.05.17.
  */
abstract class CrawlerActor extends Actor with ActorLogging {
  val cluster = Cluster(context.system)

  override def preStart() = cluster.subscribe(self,
    InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember]
  )

  override def postStop() = cluster.unsubscribe(self)
}
