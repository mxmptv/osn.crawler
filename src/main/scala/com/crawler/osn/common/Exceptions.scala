package com.crawler.osn.common

/**
  * Created by max on 31.03.17.
  */

class TaskException extends Exception

case class NoAccountFoundException(cause:String) extends TaskException
case class AccountTypeMismatchException(cause:String) extends TaskException
case class NeedRepeatTaskException(cause:String) extends TaskException
