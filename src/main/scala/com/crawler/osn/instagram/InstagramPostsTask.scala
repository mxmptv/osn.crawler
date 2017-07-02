package com.crawler.osn.instagram

import com.mongodb.util.JSON
import com.mongodb.{BasicDBList, BasicDBObject}
import com.crawler.osn.common.{InstagramTask, SaveTask, StateTask}
import com.crawler.dao.SaverInfo

import scalaj.http.Http

/**
  * Created by vipmax on 31.10.16.
  */
case class InstagramPostsTask(username: String,
                              var count: Int = 10,
                              withLocation: Boolean = false,
                              saverInfo: SaverInfo)
                             (implicit app: String)
  extends InstagramTask
    with StateTask
    with SaveTask {

  /* state */
  var offset = "0"
  var postsCounter = 0

  override def appname: String = app

  override def run(network: AnyRef) {

    var end = false
    var offset = this.offset

    while (!end) {

      val json = Http(s"https://www.instagram.com/$username/media/?max_id=$offset").execute().body

      val posts = try {
        val jsonResponse = JSON.parse(json).asInstanceOf[BasicDBObject]

        val tempPosts = jsonResponse.get("items").asInstanceOf[BasicDBList].toArray.
          map { case b: BasicDBObject =>
            b.append("key", b.getString("id"))
          }

        count -= tempPosts.length

        if (jsonResponse.getString("status") != "ok" || !jsonResponse.getBoolean("more_available") || count <= 0) end = true
        offset = tempPosts.last.getString("id")

        saveState(Map("offset" -> offset))

        if (withLocation) appendLocation(tempPosts) else tempPosts.toList
      } catch {
        case e: Exception =>
          logger.error(e)
          logger.error(json)
          List[BasicDBObject]()
      }

      postsCounter += posts.length

      logger.info(s"Found ${posts.length} posts, all $postsCounter")

      save(posts)
    }

    logger.info(s"End for task $id  found $postsCounter posts")

  }

  override def name: String = s"InstagramPostsTask(query=$username)"

  override def saveState(stateParams: Map[String, Any]) {
//    logger.debug(s"Saving state. stateParams=$stateParams")
    val offset = stateParams.getOrElse("offset", -1).toString
    this.offset = offset
  }

  def appendLocation(posts: Array[BasicDBObject]) = {
    // getting location id
    val postsIds = posts.par.map(_.getString("code")).map(getPost)

    val postsWithLocation = postsIds.par
      .map { p =>
        if(p.containsField("location") && p.get("location") != null && p.get("location").asInstanceOf[BasicDBObject].getString("id") != null){
          val locationId = p.get("location").asInstanceOf[BasicDBObject].getString("id")
          val location = getLocation(locationId)
          p.replace("location", location)
          p
        }
        else p
      }
    postsWithLocation.toList
  }
  def getLocation(locationId: String) = {
    val response = Http(s"https://www.instagram.com/explore/locations/$locationId/?__a=1").execute().body
    val location = JSON.parse(response).asInstanceOf[BasicDBObject].get("location").asInstanceOf[BasicDBObject]
    location.remove("media").asInstanceOf[BasicDBObject]
    location.remove("top_posts").asInstanceOf[BasicDBObject]
    location
  }
  def getPost(postId: String) = {
    val response = Http(s"https://www.instagram.com/p/$postId/?__a=1").execute().body
    val post = JSON.parse(response).asInstanceOf[BasicDBObject].get("media").asInstanceOf[BasicDBObject]
    post
  }
}

