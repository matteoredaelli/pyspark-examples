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
import itertools
import os, argparse

from pyspark import SparkContext
from pyspark.sql import SQLContext

def sortHashtags(hashtags):
  if hashtags is not None and len(hashtags) > 1:
    if hashtags[0] > hashtags[0]:
      (hashtags[1], hashtags[0])
    else:
      hashtags
	
if __name__ == "__main__":

  ## parsing command line parameters
  parser = argparse.ArgumentParser()
  parser.add_argument("--source", help="source path")
  parser.add_argument("--target", help="target path")
  parser.add_argument("--min_occurs", help="min occurences", default=3)

  args = parser.parse_args()

  ## connecting to hdfs data
  source_path = args.source 

  min_occurs = args.min_occurs
  if min_occurs <= 0:
     min_occurs = 3

  sc = SparkContext(appName="extraxtRelatedHashtagsFromTweets.py")
  sqlContext = SQLContext(sc)
  
  tweets = sqlContext.read.json(source_path)
  #tweets = sqlContext.jsonFile("/user/r/staging/twitter/searches/bigdata/2015/04/01.gz")
  tweets.registerTempTable("tweets")
  t = sqlContext.sql("SELECT distinct id,hashtagEntities FROM tweets")

  #related_hashtags= t.map(lambda t: map(lambda t0: t0[2], t[1])).map(lambda t: list(itertools.combinations(t, 2))).flatMap(lambda t: t).map(lambda x: ('\t'.join(unicode(i) for i in x), 1)).reduceByKey(lambda x,y: x+y).sortByKey(False).map(lambda x: '\t'.join(unicode(i) for i in x)).repartition(1)
  related_hashtags= t.map(lambda t: map(lambda t0: t0[2].lower(), t[1])) \
                     .map(lambda t: list(itertools.combinations(t, 2))) \
                     .flatMap(lambda t: t) \
                     .map(lambda t: sorted(t)) \
                     .map(lambda x: '\t'.join(unicode(i) for i in x)) \
                     .map(lambda t: (t, 1)) \
                     .reduceByKey(lambda x,y: x+y) \
                     .filter(lambda t: t[1]>=min_occurs) \
                     .sortByKey(False) \
                     .map(lambda x: '\t'.join(unicode(i) for i in x)) \
                     .repartition(1)

  ## save stats from tweets to hdfs
  related_hashtags.saveAsTextFile("%s/%s" % (args.target, "related_hashtags"))
