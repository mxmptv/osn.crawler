#!/usr/bin/env bash
pwd

rsync -aurv \
 --exclude '.idea'       \
 --exclude '.git'        \
 --exclude 'logs'        \
 --progress              \
 ./ node:/home/user/sncrawler