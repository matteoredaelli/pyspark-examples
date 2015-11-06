#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# For usage and details, see http://www.gnu.org/licenses/gpl-3.0.txt

# AUTHOR: 
#
#   matteo DOT redaelli AT gmail DOT com
#   http://www.redaelli.org/matteo
#
#
# USAGE:
#
#   spark-submit  --master yarn-client --driver-class-path /path/to/spark/assembly/target/scala-2.10/spark-assembly-1.3.0-SNAPSHOT-hadoop2.5.2.jar extractTweetsStats.py --source "/user/r/staging/twitter/searches/tyre/2014/12/*.gz" --target /tmp/tests/15

import json
import re
import sys
import time

import os,argparse

from pyspark import SparkContext
from pyspark.sql import SQLContext

def javaTimestampToString(t):
  return time.strftime("%Y-%m-%d", time.localtime(t/1000))

def cleanText(text):
  t = re.sub('["\']', ' ', unicode(text))
  return t.replace("\n"," ").replace("\t", " ").replace("\r", " ").replace("  ", " ")

def cleanTextForWordCount(text):
  # remove links
  t = re.sub(r'(http?://.+)', "", text)
  t = cleanText(t).lower()
  return re.sub('["(),-:!?#@/\'\\\]', ' ',t)

def extractSourceText(text):
  return re.sub(r'<a href=.+>(.+)</a>', "\\1", text)

def count_items(rdd, min_occurs=2, min_length=3):
  return rdd.map(lambda t: (t, 1))\
            .reduceByKey(lambda x,y:x+y)\
            .filter(lambda x:x[1] >= min_occurs)\
            .filter(lambda x:x[0] is not None and len(x[0]) >= min_length)\
            .map(lambda x:(x[1],x[0])).sortByKey(False)\
            .map(lambda x: '\t'.join(unicode(i) for i in x)).repartition(1)
     
if __name__ == "__main__":

  ## parsing command line parameters
  parser = argparse.ArgumentParser()
  parser.add_argument("--source", help="source path")
  parser.add_argument("--target", help="target path")
  parser.add_argument("--tweets", action="store_true",
                    help="include tweets")
  parser.add_argument("--min_occurs", help="min occurences", default=3)

  args = parser.parse_args()

  ## connecting to hdfs data
  source_path = args.source # /user/r/staging/twitter/searches/TheCalExperience.json/*/*/*.gz
  min_occurs = args.min_occurs
  if min_occurs <= 0:
     min_occurs = 3

  sc = SparkContext(appName="extraxtStatsFromTweets.py")
  sqlContext = SQLContext(sc)
  
  tweets = sqlContext.jsonFile(source_path)
  tweets.registerTempTable("tweets")
  t = sqlContext.sql("SELECT distinct createdAt, user.screenName, id, text, source, lang,hashtagEntities,inReplyToScreenName,source,userMentionEntities,mediaEntities FROM tweets")

  ## extraxt tweets
  
  tweets_texts = t.map(lambda t: (cleanText(t[3]),t[1],t[2])).map(lambda x: '\t'.join(unicode(i) for i in x)).repartition(1)
  ## extraxt stats from tweets
  tweets_by_days = count_items(t.map(lambda t: javaTimestampToString(t[0])))
  stats_hashtags = count_items(t.flatMap(lambda t: t[6])\
                                .map(lambda t: t[2].lower()))
       
  stats_langs = count_items(t.map(lambda t: t[5]))
  stats_replyToUser = count_items(t.map(lambda t: t[7]))
  stats_sources = count_items(t.map(lambda t: extractSourceText(t[8])))
  stats_words = count_items(t.map(lambda t: cleanTextForWordCount(t[3]))\
                                  .flatMap(lambda x: x.split()))
  stats_users_mentions = count_items(t.flatMap(lambda t: t[9])\
                                      .map(lambda t: t[3]))
  stats_media = count_items(t.flatMap(lambda t: t[10])\
                             .map(lambda t: t[5]))
       
  stats_users = count_items(t.map(lambda t: t[1]))
       
  ## extraxt tweets
  if args.tweets:
    t = sqlContext.sql("SELECT distinct user.screenName, id, text FROM tweets")
    text = t.map(lambda t: (cleanText(t[2]),t[0],t[1])).map(lambda x: '\t'.join(unicode(i) for i in x)).repartition(1)
  ## save stats from tweets to hdfs
    text.saveAsTextFile("%s/%s" % (args.target, "tweets"))

  ## extraxt users
  u = sqlContext.sql("SELECT id, user.id, user.name, user.screenName, user.location, user.description, user.followersCount, user.friendsCount, user.favouritesCount, user.statusesCount, user.lang, user.biggerProfileImageURLHttps FROM tweets")
  text = u.map(lambda t: (t[1],t)).reduceByKey(lambda x,y: x if x[1] > y[1] else y).map(lambda t: t[1]).map(lambda x: '\t'.join(unicode(i).replace("\n"," ").replace("\r"," ") for i in x)).repartition(1)
  ## save stats from tweets to hdfs
  text.saveAsTextFile("%s/%s" % (args.target, "users_details"))

  ## save stats from tweets to hdfs
  stats_hashtags.saveAsTextFile("%s/%s" % (args.target, "hashtags"))
  stats_langs.saveAsTextFile("%s/%s" % (args.target, "langs"))
  stats_replyToUser.saveAsTextFile("%s/%s" % (args.target, "reply_to_user"))
  stats_sources.saveAsTextFile("%s/%s" % (args.target, "sources"))
  stats_words.saveAsTextFile("%s/%s" % (args.target, "words"))
  stats_users.saveAsTextFile("%s/%s" % (args.target, "users"))
  stats_users_mentions.saveAsTextFile("%s/%s" % (args.target, "users_mentions"))
  stats_media.saveAsTextFile("%s/%s" % (args.target, "media"))
  tweets_by_days.saveAsTextFile("%s/%s" % (args.target, "tweets_by_day"))
