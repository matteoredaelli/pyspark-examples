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
##
## http://spark.apache.org/docs/latest/streaming-programming-guide.html
## nc -lk 9999

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


# assunzioni: da parametrizzare come parametro o letto dinamicamente da fonte
# esterna
min_occurs = 4

def signals_from_1_row_to_many(row):
  "output is (machine, date, signal_number, signal_value)"
  result = []
  for f in range(2,21):
    result = result + [(row[0], row[1], f-1, row[f])]
  return result

def isAlert(signal, value):
# assunzioni: da parametrizzare come parametro o letto dinamicamente da fonte
  defaults = [83.0, 57.0, 37.0, 57.0, 45.0, 19.0, -223.0, 20.50, 20.42, 20.48, 20.24, 20.22, 20.43, 20, 20.44, 20.39, 20.36, 20.25, 1675.0]
  soglia = 0.95
  if value == '':
     return True
  value = float(value)
  ref = defaults[signal -1]
  if value < ref - soglia*ref or value > ref + soglia*ref:
    return True
  else:
    return False
  
def isException(machine, signal):
# assunzioni: da parametrizzare come parametro o letto dinamicamente da fonte
  exceptions = [(11,19)]
  return (int(machine), signal) in exceptions 

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "SignalsAlerts")
ssc = StreamingContext(sc, 10)

# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("localhost", 9999)

# sqlContext = SQLContext(sc)
# schemaString1 = "Machine Date_time"
# schemaString2 = "signal_1 signal_2 signal_3 signal_4 signal_5 signal_6 signal_7 signal_8 signal_9 signal_10 signal_11 signal_12 signal_13 signal_14 signal_15 signal_16 signal_17 signal_18 signal_19"

# fields = [StructField("Machine", StringType(), True), StructField("date_time", StringType(), True)] + \
#          [StructField(field_name, FloatType(), True) for field_name in schemaString2.split()]
# schema = StructType(fields)
# schemaSignals = sqlContext.createDataFrame(signals, schema)
# schemaSignals.registerTempTable("signals")
# results = sqlContext.sql("SELECT * FROM signals")

# Split each line into words
all_alerts = lines.map(lambda l: l.split(",")) \
                 .flatMap(signals_from_1_row_to_many) \
                 .filter(lambda s: isAlert(s[2], s[3])) \
                 .filter(lambda s: not isException(s[0], s[2])) \
                 .map(lambda s: (s[0]+'-'+str(s[2]), [(s[1], s[3])])) \
                 .reduceByKey(lambda x, y: x + y) 

alerts = all_alerts.filter(lambda s: len(s[1]) > min_occurs)

alerts.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate

