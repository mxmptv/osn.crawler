#!/usr/bin/env bash
jps -l | grep CrawlerAgent | grep -v grep | awk '{print $1}'| xargs kill