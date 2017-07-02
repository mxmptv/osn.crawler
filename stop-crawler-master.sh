#!/usr/bin/env bash
jps -l | grep CrawlerMaster | grep -v grep | awk '{print $1}'| xargs kill