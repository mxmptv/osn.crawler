package com.crawler.osn.common

import org.joda.time.DateTime

/**
  * Created by max on 06.12.16.
  */
object Filters {
  case class TimeFilter(fromTime: DateTime = DateTime.now().minusDays(1), untilTime: DateTime = new DateTime())
  case class CountFilter(maxCount: Int = 100)
}
