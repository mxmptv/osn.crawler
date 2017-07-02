package com.crawler.core.web

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Directives._
import akka.pattern.ask
import akka.serialization.Serialization
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.crawler.core.balancers.{AppStats, GetStatsApp}
import com.crawler.core.runners.CrawlerMaster

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Created by max on 11.06.17.
  */
class CrawlerWebServer(crawlerMaster: CrawlerMaster) {
  def start() {
    implicit val system = crawlerMaster.context.system
    implicit val materializer = ActorMaterializer()

    val route =
      pathEndOrSingleSlash {
        get {
          println("connection /")
          complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, buildMainPage().mkString("\n")))
        }
      } ~
      path("app" / Segments) { appname =>
        get {

          println("connection /app " + appname)
          complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, buildAppPage(appname = appname.last).mkString("\n")))
        }
      }


    Http().bindAndHandle(route, "localhost", 5555)
    println(s"Web ыerver online at http://localhost:5555/")
  }

  def stop() {
    //TODO: implement stop logic
    println("Web server stopped")
  }

  private def buildMainPage() = {
    <html>
      <body>
        <p style="font-size: 180%">Crawler master {Serialization.serializedActorPath(crawlerMaster.self)}
          with {crawlerMaster.crawlerAgents.size} agents and
          with {crawlerMaster.crawlerClients.size} clients</p><tr></tr>

        <div>
          { crawlerMaster.crawlerClients.map{ client =>
          <p style="font-size: 160%">Crawler client {client}
            <a href={"app/"+ client.toSerializationFormat.split("/").last}>app</a>
          </p>  <tr></tr>
        }
          }
        </div>

        <div>
          { crawlerMaster.crawlerAgents.map { case (ca, info) =>
          <div>
            <p style="font-size: 150%">Crawler agent {ca.toSerializationFormat}
              with {info("workers").asInstanceOf[mutable.Set[String]].size} workers is {info("status")}</p> <tr></tr>
            { info("workers").asInstanceOf[mutable.Set[String]].map { w =>
            <p style="font-size: 100%">Crawler worker {w} </p> <tr></tr>
          }}
          </div>
        }}
        </div>
      </body>
    </html>
  }

  private def buildAppPage(appname: String) = {
    println(s"appname = $appname")
    implicit val timeout = Timeout(3 seconds)
    val future = crawlerMaster.balancer ? GetStatsApp(appname)
    val stats = Await.result(future, 5 seconds).asInstanceOf[AppStats]
    stats.appname
    println(s"stats = $stats")

    <html>
      <body>
        <div> {stats.appname}</div>
        <div> stats:
          {stats.stats.map{ stat =>
          stat.statname  + " = "+ stat.stats
        }}
        </div>
      </body>
    </html>
  }
}
