package com.crawler.osn.livejournal

import com.crawler.osn.common.LiveJournalTask
import com.crawler.dao.SaverInfo

/**
  * Created by max on 24.04.17.
  */
case class LiveJournalPostsSearchTask() extends LiveJournalTask {
  /** task name */
  override def name: String = ???

  /** task name */
  override def appname: String = ???

  /** task saver meta info. base in it crawler injects com.crawler.dao.Saver object */
  override def saverInfo: SaverInfo = ???

  /** Method for run task. Must include network logic to different OSNs */
  override def run(network: AnyRef): Unit = ???
}
