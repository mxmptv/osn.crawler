package com.crawler.core.runners
import java.util

import akka.cluster.Member
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}

import collection.JavaConversions._

/**
  * Created by Max Petrov on 18.11.15.
  */
object CrawlerConfig {

  def getConfig(clusterName: String = "crawler",
                masterIp: String = "127.0.0.1",
                myIp: String = "127.0.0.1",
                role: String): Config = {

    val port = if(role.equals(CrawlerMaster.role)) 2551 else 0
    val master = s"akka.tcp://$clusterName@$masterIp:2551"

    val actorRole = role match {
      case CrawlerMaster.role => role
      case CrawlerAgent.role => role
      case _ => clientRole(role)
    }

    ConfigFactory.empty()
      .withValue("akka.actor.provider", ConfigValueFactory.fromAnyRef("cluster"))
      .withValue("akka.remote.netty.tcp.hostname", ConfigValueFactory.fromAnyRef(myIp))
      .withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(port))
      .withValue("akka.cluster.seed-nodes", ConfigValueFactory.fromIterable(util.Arrays.asList(master)))
      .withValue("akka.cluster.roles", ConfigValueFactory.fromIterable(util.Arrays.asList(actorRole)))
      .withValue("akka.remote.maximum-payload-bytes", ConfigValueFactory.fromAnyRef("30000000 bytes"))
      .withValue("akka.remote.netty.tcp.message-frame-size", ConfigValueFactory.fromAnyRef("30000000b"))
      .withValue("akka.remote.netty.tcp.send-buffer-size", ConfigValueFactory.fromAnyRef("30000000b"))
      .withValue("akka.remote.netty.tcp.receive-buffer-size", ConfigValueFactory.fromAnyRef("30000000b"))
      .withValue("akka.remote.netty.tcp.maximum-frame-size", ConfigValueFactory.fromAnyRef("30000000b"))
      .withValue("akka.cluster.auto-down-unreachable-after", ConfigValueFactory.fromAnyRef("10s"))
      .withValue("akka.actor.warn-about-java-serializer-usage", ConfigValueFactory.fromAnyRef("false"))
  }

  def clientRole(role: String) = s"$role-${CrawlerClient.role}"

  def parseClientRole(role: String) = role.split("-")(0)

  def isCrawlerAgent(member:Member): Boolean = member.hasRole(CrawlerAgent.role)

  def isCrawlerClient(member:Member): Boolean = member.roles.head.contains(CrawlerClient.role)

}