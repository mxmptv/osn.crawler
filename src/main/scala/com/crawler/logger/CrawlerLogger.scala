package com.crawler.logger

import java.beans.Transient
import net.logstash.log4j.JSONEventLayoutV1
import org.apache.log4j.Logger
import org.joda.time.DateTime
import scala.collection.mutable

/**
  * Created by max on 24.01.17.
  */

trait CrawlerLogger {
  def info(string: Any)
  def debug(string: Any)
  def trace(string: Any)
  def error(string: Any)
  def warning(string: Any)
}

object CrawlerLoggerFactory {
  private val loggers = mutable.HashMap[String, CrawlerLogger]()

  def logger(appname: String, folder: String = "apps") = synchronized {
    if (loggers.contains(appname)) loggers(appname)
    else {
      val newLogger = new CrawlerLoggerLog4j(appname, s"$folder/$appname")
      loggers(appname) = newLogger
      newLogger
    }
  }
}

class CrawlerLoggerLog4j(loggerName: String, filename: String) extends CrawlerLogger {
  @Transient val logger = Logger.getLogger(loggerName)
  logger.addAppender(new org.apache.log4j.RollingFileAppender(new JSONEventLayoutV1(), s"logs/$filename-${new DateTime().toString("yyyy-MM-dd-HH-mm")}"))

  override def info(message: Any) = logger.info(message)
  override def debug(message: Any) = logger.debug(message)
  override def trace(message: Any) = logger.trace(message)
  override def error(message: Any) = logger.error(message)
  override def warning(message: Any) = logger.warn(message)
}
