package com.crawler.clients

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.server.Directives.{complete, get, getFromResource, path, pathEndOrSingleSlash, _}
import akka.stream.ActorMaterializer
import akka.stream.impl.PublisherSource
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.crawler.core.runners.{CrawlerAgent, CrawlerClient, CrawlerConfig, CrawlerMaster}
import com.crawler.dao.MongoSaverInfo
import com.crawler.osn.common.TaskDataResponse
import com.crawler.osn.instagram.InstagramNewGeoPostsSearchTask
import com.crawler.util.Util
import com.mongodb.{BasicDBObject, CursorType, DBObject, MongoClient}
import org.bson.Document

import scala.concurrent.duration._

/**
  * Created by max on 11.06.17.
  */
object SocialApp {
  implicit val name = "SocialApp"

  def main(args: Array[String]) {
    CrawlerMaster.main(Array())
    CrawlerAgent.main(Array())

    Thread.sleep(5000)

    val masterIp = if (args.length > 0) args(0) else Util.getCurrentIp()
    val myIp = if (args.length > 1) args(1) else Util.getCurrentIp()

    val clusterName = "crawler"
    val system = ActorSystem(clusterName,
      CrawlerConfig.getConfig(clusterName, masterIp, myIp, name)
    )
    system.actorOf(Props[SocialApp], name)
    system.whenTerminated

    val socialWebServer = new SocialWebServer(system)
    socialWebServer.start()

  }

  class SocialApp extends CrawlerClient {
    override def afterBalancerWakeUp() {
      context.system.scheduler.schedule(0 seconds, 10 seconds,
        self, "instagram")(context.dispatcher
      )
    }

    override def receiveMassage(massage: Any): Unit = massage match {
      case "instagram" =>
        val task = InstagramNewGeoPostsSearchTask(
          query = "sea",
          saverInfo = MongoSaverInfo("192.168.13.133", name, s"Posts")
        )
        send(task)
    }

    override def handleTaskDataResponse(tr: TaskDataResponse) = tr match {
      case any: TaskDataResponse =>
        println(s"any response = ${any.getClass.getSimpleName}")
    }
  }

  class SocialWebServer(system: ActorSystem) {

    def start() {

      implicit val asystem = system
      implicit val materializer = ActorMaterializer()

      val route = mainPage ~ webSocketProcessing

      Http().bindAndHandle(route, "localhost", 9999)
      println(s"Social Web server online at http://localhost:9999/")
    }

  }

  private def mainPage = {
    pathEndOrSingleSlash {
      get {
        println("connection /")
        getFromResource("index.html")
      }
    }
  }

  private def webSocketProcessing = {
    path("ws") {
      println("ws /")
//      get {
//        extractUpgradeToWebSocket { upgrade =>
//          upgrade.handleMessagesWith(
//            Sink.ignore, Source.fromIterator(() => MongoAkkaStreamTest.observable())
//        )
//        }
//      }
      handleWebSocketMessages(Flow.fromSinkAndSource(Sink.ignore, Source.fromIterator(() => MongoAkkaStreamTest.observable())))
    }
  }
}


object MongoAkkaStreamTest{
  import collection.JavaConversions._

  def main(args: Array[String]): Unit = {

//    observable()
//      .iterator().foreach(println)
  }

  def observable() = {
    val currentTimeMillis = System.currentTimeMillis() / 1000
    println(currentTimeMillis)
    new MongoClient("192.168.13.133").getDatabase("SocialApp").getCollection("Posts")
      .find(new BasicDBObject("date", new BasicDBObject("$gte", currentTimeMillis)))
      .cursorType(CursorType.TailableAwait)
      .iterator()
      .map(a => convert(a))
      .map{a => a
        println(s"new post = ${a.toJson}")
        TextMessage(a.toJson)
      }
  }

  def convert(document: Document) = {

    val returned = new Document()

    document.get("network") match {
      case "vkontakte" =>
        try { val postUrl = s"https://new.vk.com/feed?w=wall${document.get("owner_id")}_${document.get("id")}"
        returned.put("post_url", postUrl)
        } catch {case e: Exception => e.printStackTrace() }

        try { val location = document.get("geo").asInstanceOf[Document].get("coordinates").asInstanceOf[String].split(' ')
        returned.append("lat", location(0).toDouble)
        returned.append("long", location(1).toDouble)
        } catch { case e: Exception => e.printStackTrace() }

        try {
          val photo = document.get("attachments").asInstanceOf[java.util.ArrayList[Document]].filter(_.get("type") == "photo").head
            .get("photo").asInstanceOf[Document]
          val photoSizes = photo.keySet().filter(_.startsWith("photo")).map{ p => p.replace("photo_","").toInt}
          returned.append("photo_url", photo.get("photo_" + photoSizes.max))
          returned.append("icon_url", photo.get("photo_" + photoSizes.min))
          returned.append("width", photo.get("width"))
          returned.append("height", photo.get("height"))
        } catch { case e: Exception => e.printStackTrace() }

        try { val id = document.get("owner").asInstanceOf[Document].get("id")
        returned.append("user_url", s"https://vk.com/id$id")
        } catch { case e: Exception => e.printStackTrace() }

        try { returned.append("user_photo_url", {document.get("owner").asInstanceOf[Document].get("photo_100")})
        } catch { case e: Exception => e.printStackTrace() }

        try { returned.append("text", document.get("text"))
        } catch { case e: Exception => e.printStackTrace() }

        try { val owner = document.get("owner").asInstanceOf[Document]
        val username = s"${owner.get("first_name")} ${owner.get("last_name")}"
        returned.append("username", username)
        } catch { case e: Exception => e.printStackTrace() }



      case "instagram" =>
        try { val postUrl = s"https://www.instagram.com/p/${document.get("shortcode")}"
        returned.append("post_url", postUrl)
        } catch { case e: Exception => e.printStackTrace()  }

        try {
          val location = document.get("location").asInstanceOf[Document]
          returned.append("lat", location.get("lat"))
          returned.append("long", location.get("lng"))
        } catch { case e: Exception => e.printStackTrace() }


        try { val photoUrl = document.get("display_url")
        returned.append("photo_url", photoUrl)
        returned.append("icon_url", photoUrl)

        val dim = document.get("dimensions").asInstanceOf[Document]
        returned.append("width", dim.get("width"))
        returned.append("height", dim.get("height"))
        } catch { case e: Exception => e.printStackTrace() }


        try { val username = document.get("owner").asInstanceOf[Document].get("username")
        returned.append("user_url", s"https://www.instagram.com/$username")
        } catch { case e: Exception => e.printStackTrace() }

        try { returned.append("user_photo_url", {document.get("owner").asInstanceOf[Document].get("profile_pic_url")})
        } catch { case e: Exception => e.printStackTrace() }

//        data['edge_media_to_caption']['edges'][0]['node']['text']
        try { returned.append("text", document.get("edge_media_to_caption").asInstanceOf[Document]
                                              .get("edges").asInstanceOf[java.util.ArrayList[Document]].head
                                              .get("node").asInstanceOf[Document]
                                              .getString("text"))
        } catch { case e: Exception => e.printStackTrace() }

        try { val owner = document.get("owner").asInstanceOf[Document]
        returned.append("username", owner.get("username"))
        } catch { case e: Exception => e.printStackTrace() }

    }

    returned
  }
}
