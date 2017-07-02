package com.crawler.osn.common

import java.beans.Transient

import akka.actor.{ActorRef, ActorSelection}
import com.mongodb.BasicDBObject
import com.crawler.dao.{Saver, SaverInfo}
import com.crawler.logger.CrawlerLogger
import com.crawler.util.Util

import scalaj.http.HttpRequest


/**
  * Created by vipmax on 10.08.16.
  **/

trait Task {

  /** Unique task id */
  val id = java.util.UUID.randomUUID.toString.substring(0, 10)

  /** task name */
  def name: String

  /** task name */
  def appname: String

  /** task type */
  def taskType() = this.getClass.getSimpleName

  /** task attempt */
  var attempt = 0

  /** task proxy */
  @Transient var proxy: CrawlerProxy = null

  /** crawler balancer */
  @Transient var balancer: AnyRef  = null

  /** task logger. Must be injected by crawler */
  @Transient var logger: CrawlerLogger = null

  /** task saver meta info. base in it crawler injects com.crawler.dao.Saver object */
  def saverInfo: SaverInfo

  /** task saver. Must be injected by crawler */
  @Transient var saver: Saver = null

  /** task account (osn's credential). Can be injected by crawler */
  var account: Account = null

  /** Method for run task. Must include network logic to different OSNs */
  def run(network: AnyRef)

  /** experimental, for data normalization, should contains field regexps
    * use List(".") for all fields to be presented
    * use List("id$") for "id" field to be presented
    * use List("medium.url") for "medium*url" field to be presented
    * default there is no denormalization
    * */
  var dataSchema: List[String] = List()

  /** experimental, for WF generation */
  var onResult: (Array[BasicDBObject]) => Unit = _
  var onException: (Array[BasicDBObject]) => Unit = _

  def next(tasks:Array[Task]) {
    println(s"sending ${tasks.size} tasks to balancer $balancer")

    tasks.foreach{ task =>
      balancer match {
        case  b: ActorRef => b ! task
        case  b: ActorSelection => b ! task
        case  _ => logger.warning("balancer not found")
      }
    }

  }
}


trait TaskDataResponse {
  def task: Task
  def resultData: Array[BasicDBObject]
}

case class TaskStatusResponse(task: Task, status: String, exception: Exception)

trait ResponseTask extends Task {
  var isStream = false
  val responseActor: AnyRef = null

  def response(responseActor: AnyRef, response: TaskDataResponse) {
    println(response, responseActor)
    responseActor match {
      case  ar: ActorRef => ar ! response
      case  as: ActorSelection => as ! response
      case  _ => logger.warning("responseActor not found")
    }
  }

  def response(response: TaskDataResponse) {
    this.response(this.responseActor, response)
  }
}


trait SaveTask extends Task {
  def save(datas: Any) {
    if(saver != null ) {
      datas match {
        case many: Iterable[BasicDBObject] if many.nonEmpty =>
          if(dataSchema.isEmpty)
            saver.saveMany(many)
          else {
            val data = many.map( m => Util.denorm(m, dataSchema))
            saver.saveMany(data)
          }
        case one: BasicDBObject if one != null =>
          saver.saveOne(one)
      }
    }
  }
}


trait StateTask extends Task {
  var state = Map[String, Any]()

  def saveState(newState: Map[String, Any]): Unit = synchronized {
    state = newState
  }
}

trait FrequencyLimitedTask {
  def sleep(time: Int = 3000): Unit = {
    Thread.sleep(time)
  }
}



trait TwitterTask extends Task {
  def newRequestsCount(): Int
}

trait VkontakteTask extends Task {

  override def run(network: AnyRef = null) {

    /* using  balancer account first */
    var account: Account = network match {
      case acc:VkontakteAccount => acc
      case _ => null
    }

    /* can be overrided by own user account */
    if(this.account != null) account = this.account

    if(this.logger == null) throw new NullPointerException("logger is null")
//    if(account == null) throw NoAccountFoundException("account is null")
//    if(!account.isInstanceOf[VkontakteAccount]) throw AccountTypeMismatchException("account is not VkontakteAccount")

    extract(account.asInstanceOf[VkontakteAccount])

  }

  def extract(account: VkontakteAccount)

  def exec(httpRequest: HttpRequest): String = {
    var httpReq = account match {
      case VkontakteAccount(accessToken) =>
        logger.debug(s"exec task.id $id with access_token=$accessToken")
        httpRequest.param("access_token", accessToken)
      case null => httpRequest
    }

    httpReq = proxy match {
      case p:CrawlerProxy =>
        logger.debug(s"exec task.id $id with proxy=$proxy")
        httpReq.proxy(p.url, p.port.toInt, p.proxyType match {
          case "http" => java.net.Proxy.Type.HTTP
          case "socks" => java.net.Proxy.Type.SOCKS
        })
      case null => httpReq
    }

    logger.debug(s"${httpReq.url}?${httpReq.params.map { case (p, v) => s"$p=$v" }.mkString("&")}")
    val json = httpReq.timeout(60 * 1000 * 10, 60 * 1000 * 10).execute().body.toString

    if(json.contains("error_code")) logger.error(json)

    json
  }
}


trait InstagramTask extends Task

trait YoutubeTask extends Task {
  def exec(httpRequest: HttpRequest): String = {
    var httpReq = account match {
      case YoutubeAccount(key) =>
        logger.debug(s"exec task ${taskType()} task.id $id with key=$key")
        httpRequest.param("key", key)
      case null => httpRequest
    }

    httpReq = proxy match {
      case p:CrawlerProxy =>
        logger.debug(s"exec task ${taskType()} task.id $id with proxy=$proxy")
        httpReq.proxy(p.url, p.port.toInt, p.proxyType match {
          case "http" => java.net.Proxy.Type.HTTP
          case "socks" => java.net.Proxy.Type.SOCKS
        })
      case null => httpReq
    }

    logger.debug(s"${httpReq.url}?${httpReq.params.map { case (p, v) => s"$p=$v" }.mkString("&")}")
    val json = httpReq.timeout(60 * 1000 * 10, 60 * 1000 * 10).execute().body.toString

    if(json.contains("error")) logger.error(json)

    json
  }
}

trait LiveJournalTask extends Task
trait FacebookTask extends Task
