#!/usr/bin/env bash
java -cp target/osn.crawler-1.0.jar:target/libs/* com.crawler.core.runners.CrawlerAgent $@