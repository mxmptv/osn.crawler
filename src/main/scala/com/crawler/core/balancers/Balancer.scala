package com.crawler.core.balancers

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import com.crawler.core.actors.WorkerActor
import com.crawler.core.actors.WorkerActor.SimpleWorkerTaskRequest
import com.crawler.osn.common.Task
import com.crawler.core.runners.CrawlerAgent.WorkerUp
import com.crawler.logger.CrawlerLoggerFactory
import com.crawler.util.Util.{Continue, Stop, _}
import scala.concurrent.duration._
import scala.collection.mutable
/**
  * Created by max on 02.12.16.
  */

case class InitBalancer()
case class UpdateSlots()

case class TypedTaskRequest(tokens: collection.mutable.Map[String, Int],
                            previousTask: Task = null,
                            worker: ActorRef)

case class WorkerIsReady(worker: ActorRef)

case class KillApp(appname:String)
case class GetStatsApp(appname:String)

case class AppStat(statname: String, stats: Map[String, Any])
case class AppStats(appname:String, stats: List[AppStat])

case class App(name:String, var quantumCount:Int = 1, var quantumLeft:Int = 1,
               var avgExecutionTime:Double = -1){

  val tasksByType: mutable.LinkedHashMap[String, mutable.LinkedHashSet[Task]] = mutable.LinkedHashMap[String, mutable.LinkedHashSet[Task]]()
  val taskList: mutable.LinkedHashSet[Task] = mutable.LinkedHashSet[Task]()

  /*
   required for two-level quantum typed balancer
   */
  private var _currtaskTypeIndex: Int = 0

  def addTask(task:Task) {
    val taskType = task.taskType()
    if (!tasksByType.contains(taskType)) {
      tasksByType(taskType) = mutable.LinkedHashSet[Task]()
    }
    tasksByType(taskType).add(task)
    taskList.add(task)
  }

  def getTaskByType(taskType: String): Option[Task] = {
    tasksByType.get(taskType).map {x => x.find(_ => true).get}
  }

  def removeTask(task:Task): Unit = {
    val taskType = task.taskType()
    tasksByType(taskType).remove(task)
    if (tasksByType(taskType).isEmpty){
      tasksByType.remove(taskType)
      // we don't need to move currTaskTypeIndex because it is already "moved"
      // but we need to check for end of sequence
      _currtaskTypeIndex = if (_currtaskTypeIndex < tasksByType.size) _currtaskTypeIndex else 0
    }
    taskList.remove(task)
  }

  def tasks: List[Task] = taskList.toList

  def taskTypes() = tasksByType.keys.toList

  def updateCurrTaskTypeIndex(index:Int) {
    _currtaskTypeIndex = index
  }

  def currTaskTypeIndex = _currtaskTypeIndex

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case App(n,_,_,_) if n == name => true
      case _ => false
    }
  }

  override def hashCode(): Int = name.hashCode

  override def toString: String = {
    s" Appname=$name tasks=" + taskList.groupBy(_.taskType()).map{case (tasktype, tasks) => (tasktype, tasks.size)}
  }
}


object Balancer {
  val name = "SimpleBalancer"
}

class Balancer extends Actor {
  val logger = CrawlerLoggerFactory.logger("SimpleBalancer","core/balancer")

  val freeWorkers = mutable.Map[String, mutable.Set[ActorRef]]()
  val apps = mutable.ListBuffer[App]()
  val taskCounters = mutable.Map[String, Int]()
  var currentAppIndex = 0

  context.system.scheduler.schedule(30 seconds, 1 minute, self, "print stats")(context.dispatcher)

  override def preStart(): Unit = logger.debug("SimpleBalancer preStart")

  override def postStop(): Unit = logger.debug("SimpleBalancer postStop")

  override def receive: Receive = {
    case  WorkerUp(workerPath) =>
      logger.trace(s"Got SimpleWorkerAdded $workerPath")
      context.actorSelection(workerPath) ! InitBalancer()

    case workerTaskRequest: SimpleWorkerTaskRequest =>
      logger.trace(s"Got SimpleWorkerTaskRequest($workerTaskRequest)")

      val maybeTask = dequeueTask(workerTaskRequest)

      maybeTask match {
        case Some(task) =>
          logger.trace(s"Found task=${task.name} for workerTaskRequest. Sending to worker $sender")
          sender ! task

        case None =>
          logger.trace(s"Task not found for workerTaskRequest $workerTaskRequest")
          addFreeWorker(sender, workerTaskRequest)
      }


    case task: Task =>
      logger.trace(s"Got Task(${task.name})")

      val freeWorker = getFreeWorker(task.taskType())

      freeWorker match {
        case Some(worker) =>
          logger.trace(s"Sending task ${task.name} to worker $worker")

          worker ! task
          removeFreeWorker(worker, task.taskType())

        case None =>
          logger.trace(s"freeWorker not found for task type: ${task.taskType()}")

          enqueueTask(task)
      }

    case GetStatsApp(appname) =>
      val app = apps.find(_.name == appname)
      app match {
        case Some(a) =>
          val tasktypes: Map[String, Int] = a
          .taskList.groupBy(_.taskType())
          .map{case (tasktype, tasks) => (tasktype, tasks.size) }

          val stat = AppStat("tasktypes", Map("tasktypes" -> tasktypes))
          val appStats = List(stat)
          sender ! AppStats(appname, appStats)
        case None =>
          val stat = AppStat("tasktypes", Map("tasktypes" -> Map()))
          val appStats = List(stat)
          sender ! AppStats(appname, appStats)
      }

    case KillApp(appnameForKill) =>
      logger.info(s"Killing app=$appnameForKill")
      apps -= App(appnameForKill)

    case "print stats" =>
      logger.info(s"actual tasks count = " + apps.map{ app => app.tasks.length}.sum)
      logger.info("apps: " + apps.mkString(", "))

    case "get stats" =>
      logger.info(s"actual tasks count = " + apps.map{ app => app.tasks.length}.sum)
      logger.info("apps: " + apps.mkString(", "))

    case _ => throw new Exception(s"Unknown type of message")
  }


  def getFreeWorker(slot: String): Option[ActorRef] = {
    if(freeWorkers.contains("anytask") && freeWorkers("anytask").nonEmpty) return getAndRemoveFreeWorker("anytask")
    if (!freeWorkers.contains(slot))    return None

    val freeWorker = getAndRemoveFreeWorker(slot)
    return freeWorker
  }

  private def getAndRemoveFreeWorker(slot: String) = {
    val workers = freeWorkers(slot)
    if (workers.nonEmpty) {
      val worker = workers.head
      workers -= worker

      // if take the actor for executing a task,
      // we remove it from anywhere else cause it blocks the actot
      for ((slot, workers) <- freeWorkers) {
        workers.remove(worker)
      }

      Option(worker)
    }
    else {
      None
    }
  }

  def addFreeWorker(freeWorker: ActorRef, workerTaskRequest: SimpleWorkerTaskRequest) = {
    val tt = Option(workerTaskRequest.task) match {
      case Some(t) => t.taskType()
      case None => "anytask"
    }

    if (!freeWorkers.contains(tt)) freeWorkers.put(tt, mutable.Set[ActorRef]())
    freeWorkers(tt) += freeWorker
  }

  def enqueueTask(task: Task) = {
    val app = App(task.appname)

    if (!apps.contains(app)) {
      app.addTask(task)
      apps += app
    }
    else {
      val currentApp = apps.find(_.equals(app)).get
      currentApp.addTask(task)
    }

    val taskType = task.getClass.toString
    if (!taskCounters.contains(taskType)){
      taskCounters.put(taskType, 0)
    }
    taskCounters(taskType) += 1
  }

  def dequeueTask(workerTaskRequest: SimpleWorkerTaskRequest): Option[Task] = {
    val appAndTasks = findApp(workerTaskRequest)

    val task = appAndTasks match {
      case Some((app, availableTaskTypes)) =>
        val task = findTask(app, availableTaskTypes)
        task

      case None =>
        None
    }

    // update appropriate counters
    task match {
      case Some(t) =>
        val (app, _) = appAndTasks.get
        app.removeTask(t)

        val taskType = t.getClass.toString
        taskCounters(taskType) -= 1
      case None =>
    }

    // calculate probabilities and choose the task
    // returns one of the tasks back
    task
  }

  def findApp(taskRequest: SimpleWorkerTaskRequest): Option[(App, Set[String])] = {
    if (apps.isEmpty)
      return None

    val (currIndex, app) = ringLoop(apps, start = currentAppIndex) { app =>
      // check if tokens are matching
      val availableTaskTypes = app.taskTypes().toSet
      if (availableTaskTypes.nonEmpty) {
        Stop((app, availableTaskTypes))
      } else {
        Continue
      }
    }

    currentAppIndex = if (currIndex + 1 < apps.length) currIndex + 1 else 0
    app
  }

  def findTask(app: App, availableTaskTypes: Set[String]): Option[Task] = {
    val sortedTaskTypes = app.taskTypes()
    val curr = app.currTaskTypeIndex

    val (currIndex, task) = ringLoop(sortedTaskTypes, start = curr) { taskType =>
      // check if tokens are matching
      if (availableTaskTypes.contains(taskType)) {
        val task = app.getTaskByType(taskType)
        task match {
          case Some(t) =>
            Stop(t)
          case None =>
            Continue
        }
      } else {
        Continue
      }
    }

    val newCurrIndex = if (currIndex + 1 < sortedTaskTypes.length) currIndex + 1 else 0
    app.updateCurrTaskTypeIndex(newCurrIndex)
    task
  }

  def removeFreeWorker(worker: ActorRef, tasktype:String) = {
    if(freeWorkers.contains(tasktype))freeWorkers(tasktype) -= worker
  }
}
