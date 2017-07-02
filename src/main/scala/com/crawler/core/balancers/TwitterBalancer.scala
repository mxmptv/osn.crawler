package com.crawler.core.balancers

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import com.crawler.core.actors.TwitterSimpleWorkerActor
import com.crawler.core.actors.twitter.TwitterSequentialTypedWorkerActor.TwitterTypedWorkerTaskRequest
import com.crawler.osn.common.{Task, TwitterTask}
import com.crawler.core.runners.CrawlerAgent.WorkerUp
import com.crawler.util.Util
import com.crawler.util.Util.{Continue, Stop, _}

import scala.collection.mutable

/**
  * Created by vipmax on 29.11.16.
  */
object TwitterBalancer {
  val name = "TwitterBalancer"
}

class TwitterBalancer extends Actor {
  val logger = Logger.getLogger(this.getClass)

  val freeWorkers = mutable.Map[String, mutable.Set[ActorRef]]()
  val apps = mutable.ListBuffer[App]()
  val taskCounters = mutable.Map[String, Int]()
  var currentAppIndex = 0
  var actualTasksCount = 0

  override def preStart(): Unit = logger.debug("TwitterBalancer preStart")

  override def postStop(): Unit = logger.debug("TwitterBalancer postStop")

  override def receive: Receive = {
    case  WorkerUp(workerPath) =>
      logger.info(s"Got WorkerAdded $workerPath")
      context.actorSelection(workerPath) ! InitBalancer()

    case message: String =>
      logger.info(s"Got message $message from $sender")

    case workerTaskRequest: TwitterTypedWorkerTaskRequest =>
      logger.debug(s"Got ${workerTaskRequest.getClass.getSimpleName}($workerTaskRequest)")

      val maybeTask = dequeueTask(workerTaskRequest)

      maybeTask match {
        case Some(task) =>
          logger.trace(s"Found task=${task.name} for workerTaskRequest. Sending to worker $sender")
          sender ! task

        case None =>
          logger.trace(s"Task not found for workerTaskRequest $workerTaskRequest")
          addFreeWorker(sender, workerTaskRequest)
      }


    case task:TwitterTask  =>
      logger.trace(s"Got ${task.getClass.getSimpleName}(${task.name})")

      val freeWorker = getFreeWorker(task.taskType())

      freeWorker match {
        case Some(worker) =>
          logger.trace(s"Sending task ${task.name} to worker $worker")

          worker ! task
          removeFreeWorker(worker,task.taskType())
          actualTasksCount += 1

        case None =>
          logger.trace(s"freeWorker not found for task type: ${task.taskType()}")

          enqueueTask(task)
          actualTasksCount += 1
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

    case _ => throw new Exception(s"Unknown type of message")
  }


  def getFreeWorker(taskType: String): Option[ActorRef] = {
    if (!freeWorkers.contains(taskType))
      return None

    val workers = freeWorkers(taskType)
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

  /* for each  free slot adding freeWorker */
  def addFreeWorker(freeWorker: ActorRef, workerTaskRequest: TwitterTypedWorkerTaskRequest) = {
    workerTaskRequest.freeSlots.foreach{ case freeSlot =>
      if (!freeWorkers.contains(freeSlot)) freeWorkers.put(freeSlot, mutable.Set[ActorRef]())
      freeWorkers(freeSlot) += freeWorker
    }
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

    val balanserState = apps.map(a => (a.name, s"tasks{${a.tasksByType.map { case (tT, tQueue) => s"$tT ->${tQueue.size}" }.mkString("; ")}}")).mkString("\n")
    logger.trace("balanserState = "+balanserState)
  }

  def dequeueTask(workerTaskRequest: TwitterTypedWorkerTaskRequest): Option[Task] = {
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

    task
  }

  def findApp(taskRequest: TwitterTypedWorkerTaskRequest): Option[(App, Set[String])] = {
    if (apps.isEmpty)
      return None

    val (currIndex, app) = ringLoop(apps, start = currentAppIndex) { app =>
      // check if tokens are matching
      val availableTaskTypes = app.taskTypes().toSet.intersect(taskRequest.freeSlots)
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

  def removeFreeWorker(worker: ActorRef, taskType: String) = {
    freeWorkers(taskType) -= worker
  }
}


