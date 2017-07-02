package com.crawler.osn.common

/**
  * Created by vipmax on 31.10.16.
  */

trait Account
case class MockAccount() extends Account
case class TwitterAccount(key: String, secret: String, token: String, tokenSecret: String) extends Account
case class VkontakteAccount(accessToken: String) extends Account
case class InstagramAccount() extends Account
case class YoutubeAccount(key: String) extends Account
