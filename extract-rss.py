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
#   spark-submit  --master yarn-client --driver-class-path /path/to/spark/assembly/target/scala-2.10/spark-assembly-1.3.0-hadoop2.5.2.jar extract-rss.py --source "/user/r/staging/twitter/searches/tyre/2014/12/*.gz" --target /tmp/tests/15

import re
import sys
import time

import xml.etree.cElementTree as ET

import os,argparse
from datetime import datetime

from pyspark import SparkContext

def clean_string(string):
  try:
    return string.replace("\t", " ").replace("\n", " ").replace("'", " ").replace('"', " ")
  except:
    return string

def html_to_text(html):
  try:
    return re.sub(r"<.*?>", "", html)
  except:
    return html

def rss_string_to_xml_object(line):
  line = line.encode('utf8', 'replace')
  tree = ET.ElementTree(ET.fromstring(line))
  return tree.getroot()

def safe_root_find(root, field):
  try:
    return root.find(field).text
  except:
    return ""

def rss_string_to_list(line):
  root = rss_string_to_xml_object(line)
  title = clean_string(safe_root_find(root, 'title'))
  description = clean_string(html_to_text(safe_root_find(root, 'description')))
  pubDate = safe_root_find(root, 'pubDate')[5:16]
## carbuzz has not pubDate field... :-(
  if pubDate == "":
	pubDate = "%s-%s-%s" % (args.year, args.month, args.day)
  else:
  	pubDate = datetime.strptime(pubDate, '%d %b %Y').strftime("%Y-%m-%d")

  source = safe_root_find(root, 'rss_source')
  link = safe_root_find(root, 'link')
  language = safe_root_find(root, 'rss_language')
  category = safe_root_find(root, 'rss_category')
  return (language,source,category,pubDate,title,link,description)          

def count_items_and_save(rdd, field_name, field_count, target_path, min_occurs=1):
  rdd.map(lambda t: (t[field_count], 1))\
      .reduceByKey(lambda x,y:x+y)\
      .filter(lambda x:x[1] >= min_occurs)\
      .map(lambda x:(x[1],x[0])).sortByKey(False)\
      .map(lambda x: '\t'.join(unicode(i) for i in x)).repartition(1)\
      .saveAsTextFile("%s/%s" % (target_path, field_name))

if __name__ == "__main__":

  ## parsing command line parameters
  parser = argparse.ArgumentParser()
  parser.add_argument("--source", help="source path")
  parser.add_argument("--target", help="target_path")
  parser.add_argument("--year", help="year")
  parser.add_argument("--month", help="month")
  parser.add_argument("--day", help="day")

  args = parser.parse_args()

  ## if Day is None, I'll process the file related to yesterday
  if args.day is None:
    yesterday = datetime.datetime.now() - datetime.timedelta(days = 1)
    args.year = yesterday.strftime('%Y')
    args.month = yesterday.strftime('%m')
    args.day = yesterday.strftime('%d')

  ## connecting to hdfs data
  source_files = "%s/%s/%s/%s.gz" % (args.source, args.year, args.month, args.day)
  target_path = args.target

  sc = SparkContext(appName="rss-to-html-report.py")
  
  rdd = sc.textFile(source_files).distinct().map(rss_string_to_list)
  
  ## extract stats
  # (language,source,category,pubDate,title,link,description)          
  
  count_items_and_save(rdd, 'language', 0, target_path)
  count_items_and_save(rdd, "source", 1, target_path)
  count_items_and_save(rdd, 'category', 2, target_path)
  count_items_and_save(rdd, "pubDate", 3, target_path)

  ## extract texts
  rdd.map(lambda x: '\t'.join(unicode(i).replace("\n"," ") for i in x)).repartition(1).saveAsTextFile("%s/%s" % (target_path, "news"))
