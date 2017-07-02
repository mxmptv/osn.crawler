package com.crawler.osn.facebook

import scalaj.http.Http

/**
  * Created by max on 14.12.16.
  */
object FacebookTests {
  def main(args: Array[String]): Unit = {
    val accessToken = "EAACEdEose0cBACQbrEdBZBOmLEjZAKze41jPHycZCDxF7KkMZA0K2p6ZAXqTGFBbrXdZCIfkKSQiPSmUcQsTMTlH2EyYp9AqmbLZASpKkKJpwKVQyrPHMdIRpp9T6K86csfIJUwp88bdursntnWZAkYWvi5cGInNCuRIQigeWOsuhgZDZD"

    val res = Http("https://graph.facebook.com/search?")
      .param("type", "user")
      .param("q", "lebron james")
//      .param("count", "1000")
//      .param("offset", offset.toString)
//      .param("v", "5.8")
      .param("access_token", accessToken)
      .timeout(60 * 1000 * 10, 60 * 1000 * 10)
      .execute().body

    println(res)

//    no posts search in facebook :(
    val res2 = Http("https://graph.facebook.com/search?")
      .param("type", "post")
      .param("q", "lebron james")
//      .param("count", "1000")
//      .param("offset", offset.toString)
//      .param("v", "5.8")
      .param("access_token", accessToken)
      .timeout(60 * 1000 * 10, 60 * 1000 * 10)
      .execute().body

    println(res2)

  }
}
