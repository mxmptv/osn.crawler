package com.crawler.osn.instagram

import com.mongodb.util.JSON
import com.mongodb.{BasicDBList, BasicDBObject}
import com.crawler.osn.common.{InstagramTask, SaveTask, StateTask}
import com.crawler.dao.SaverInfo

import scalaj.http.Http

/**
  * Created by vipmax on 31.10.16.
  */
case class InstagramSearchPostsTask(query: String,
                                    var count: Int = 10,
                                    saverInfo: SaverInfo)
                                   (implicit app: String)
  extends InstagramTask
    with SaveTask
    with StateTask {

  override def appname: String = app

  override def run(network: AnyRef) {
    var end = false

    while (!end) {
      val request = this.state.get("offset") match {
        case Some(next: String) => Http(s"https://www.instagram.com/explore/tags/$query/?__a=1").param("max_id", next)
        case None => Http(s"https://www.instagram.com/explore/tags/$query/?__a=1")
      }

      val (posts, hasNext, nextOffset) = parseResponse(request.execute().body)
      logger.debug(s"Found ${posts.length} posts. taskid=$id")

      count -= posts.length
      end = !hasNext || count <= 0

      saveState(Map("offset" -> nextOffset))
      save(posts)
    }
  }

  private def parseResponse(json: String) = {
    val jsonResponse = JSON.parse(json).asInstanceOf[BasicDBObject]

    val media = jsonResponse.get("tag").asInstanceOf[BasicDBObject]
      .get("media").asInstanceOf[BasicDBObject]

    val pageInfo = media.get("page_info").asInstanceOf[BasicDBObject]

    val posts = media.get("nodes").asInstanceOf[BasicDBList].toArray
      .map { case b: BasicDBObject =>
        b.put("key", b.getString("id"))
        b.put("network", "instagram")
        b.put("date", b.get("taken_at_timestamp"))
        b.remove("taken_at_timestamp")
        b
      }
    (posts, pageInfo.getBoolean("has_next_page"), pageInfo.getString("end_cursor"))
  }

  override def name: String = s"InstagramSearchPostsTask(query=$query)"
}

case class InstagramNewGeoPostsSearchTask(query: String,
                                          saverInfo: SaverInfo)
                                         (implicit app: String)
  extends InstagramTask
    with SaveTask
    with StateTask {

  override def appname: String = app

  override def run(network: AnyRef) {
    val posts = searchPosts(query)
    val postsWithLocation = appendLocation(posts)
    postsWithLocation.foreach(save)
  }

  def searchPosts(tag: String) = {
    val stringResponse = Http(s"https://www.instagram.com/explore/tags/$tag/?__a=1").timeout(60000,60000).execute().body
    val jsonResponse = JSON.parse(stringResponse).asInstanceOf[BasicDBObject]

    val posts = jsonResponse.get("tag").asInstanceOf[BasicDBObject].get("media").asInstanceOf[BasicDBObject]
      .get("nodes").asInstanceOf[BasicDBList].toArray.map(_.asInstanceOf[BasicDBObject])
    posts
  }

  def getPost(postId: String) = {
    val response = Http(s"https://www.instagram.com/p/$postId/?__a=1").execute().body
    val post = JSON.parse(response).asInstanceOf[BasicDBObject]
      .get("graphql").asInstanceOf[BasicDBObject]
      .get("shortcode_media").asInstanceOf[BasicDBObject]
    post
  }

  def appendLocation(posts: Array[BasicDBObject]) = {
    val postsIds = posts.par.map{p => try { getPost(p.getString("code"))} catch { case e:Exception => p }}
    postsIds.par
      .map { p =>
        try {
          val locationId = p.get("location").asInstanceOf[BasicDBObject].getString("id")
          val location = getLocation(locationId)
          p.replace("location", location)
        } catch { case _ => }

        p.put("key", p.getString("id"))
        p.put("query", query)
        p.put("network", "instagram")
        p.put("date", p.get("taken_at_timestamp"))
        p.remove("taken_at_timestamp")
        p
      }.toArray
  }

  def getLocation(locationId: String) = {
    val response = Http(s"https://www.instagram.com/explore/locations/$locationId/?__a=1").execute().body
    val location = JSON.parse(response).asInstanceOf[BasicDBObject].get("location").asInstanceOf[BasicDBObject]
    location.remove("media").asInstanceOf[BasicDBObject]
    location.remove("top_posts").asInstanceOf[BasicDBObject]
    location
  }


  override def name: String = s"InstagramSearchPostsTask(query=$query)"
}

