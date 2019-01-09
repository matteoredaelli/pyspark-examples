from pyspark.sql import *
from pyspark.sql import functions as F

df = spark.read.csv("/tmp/*.csv", header=True, sep=";")
df.count()
df.columns

df1 = spark.read.csv("/tmp/Artikelliste_Affnet_new.csv", header=True, sep=";")
b1=df1.select("Hersteller").withColumnRenamed("Hersteller","brand").withColumn("brand", F.upper(F.col("brand"))).distinct().sort("brand")

df2 = spark.read.csv("/tmp/ArtikellisteMot_Affnet_new.csv", header=True, sep="|")
b2=df2.select("Hersteller").withColumnRenamed("Hersteller","brand").withColumn("brand", F.upper(F.col("brand"))).distinct().sort("brand")

df3 = spark.read.csv("/tmp/Artikelliste_Zanox_IT.csv", header=True, sep="\t")
b3=df3.select("Hersteller").withColumnRenamed("Hersteller","brand").withColumn("brand", F.upper(F.col("brand"))).distinct().sort("brand")

df4 = spark.read.csv("/tmp/Artikelliste_HURRA_IT.csv", header=True, sep="\t")
b4=df4.select("Hersteller").withColumnRenamed("Hersteller","brand").withColumn("brand", F.upper(F.col("brand"))).distinct().sort("brand")

df5 = spark.read.csv("/tmp/Artikelliste_HURRA_FR.csv", header=True, sep="\t")
b5=df5.select("Hersteller").withColumnRenamed("Hersteller","brand").withColumn("brand", F.upper(F.col("brand"))).distinct().sort("brand")

b1.union(b2).union(b3).union(b4).union(b5).groupBy("brand").count().orderBy("count", ascending=False).show()

b1.union(b2).union(b3).union(b4).union(b5).withColumn("brand", F.trim(F.col("brand"))).distinct().sort("brand").write.csv("brand")
