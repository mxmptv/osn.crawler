package com.crawler.osn.common

import java.net

import com.crawler.util.Util
import org.jsoup.Jsoup

import scalaj.http.Http

/**
  * Created by max on 12.05.17.
  */

case class CrawlerProxy(proxyType: String, url: String, port: String)

object ProxyUtils {
  def main(args: Array[String]): Unit = {
    val httpProxies = Util.getHttpProxies()
    httpProxies.foreach(println)
  }
}