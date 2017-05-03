from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import *
import datetime
from pyspark.sql.types import *
import pandas as pd
from pyspark.sql import Row
from datetime import date
import numpy as np
import urllib
import json
import pylab as pl

spark = SparkSession\
.builder\
.appName("Python Spark SQL basic example")\
.config("spark.some.config.option", "some-value")\
.getOrCreate()

#2013 Data
cb713 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/2013-07 - Citi Bike trip data.csv", inferSchema='true')	

cb813 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/2013-08 - Citi Bike trip data.csv", inferSchema='true')

cb913 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/2013-09 - Citi Bike trip data.csv", inferSchema='true')

cb1013 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/2013-10 - Citi Bike trip data.csv", inferSchema='true')

cb1113 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/2013-11 - Citi Bike trip data.csv", inferSchema='true')

cb1213 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/2013-12 - Citi Bike trip data.csv", inferSchema='true')	

#2014 Data
cb114 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/2014-01 - Citi Bike trip data.csv", inferSchema='true')

cb214 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/2014-02 - Citi Bike trip data.csv", inferSchema='true')

cb314 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/2014-03 - Citi Bike trip data.csv", inferSchema='true')

cb414 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/2014-04 - Citi Bike trip data.csv", inferSchema='true')

cb514 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/2014-05 - Citi Bike trip data.csv", inferSchema='true')

cb614 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/2014-06 - Citi Bike trip data.csv", inferSchema='true')

cb714 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/2014-07 - Citi Bike trip data.csv", inferSchema='true')

cb814 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/2014-08 - Citi Bike trip data.csv", inferSchema='true')

cb914 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/201409-citibike-tripdata.csv", inferSchema='true')

cb1014 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/201410-citibike-tripdata.csv", inferSchema='true')

cb1114 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/201411-citibike-tripdata.csv", inferSchema='true')

cb1214 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/201412-citibike-tripdata.csv", inferSchema='true')


#2015 Data	
cb115 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/201501-citibike-tripdata.csv", inferSchema='true')

cb215 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/201502-citibike-tripdata.csv", inferSchema='true')

cb315 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/201503-citibike-tripdata.csv", inferSchema='true')

cb415 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/201504-citibike-tripdata.csv", inferSchema='true')

cb515 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/201505-citibike-tripdata.csv", inferSchema='true')

cb615 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/201506-citibike-tripdata.csv", inferSchema='true')

cb715 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/201507-citibike-tripdata.csv", inferSchema='true')

cb815 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/201508-citibike-tripdata.csv", inferSchema='true')

cb915 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/201509-citibike-tripdata.csv", inferSchema='true')

cb1015 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/201510-citibike-tripdata.csv", inferSchema='true')

cb1115 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/201511-citibike-tripdata.csv", inferSchema='true')

cb1215 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/201512-citibike-tripdata.csv", inferSchema='true')

#2016 Data

cb116 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/201601-citibike-tripdata.csv", inferSchema='true')

cb216 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/201602-citibike-tripdata.csv", inferSchema='true')

cb316 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/201603-citibike-tripdata.csv", inferSchema='true')

cb416 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/201604-citibike-tripdata.csv", inferSchema='true')

cb516 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/201605-citibike-tripdata.csv", inferSchema='true')

cb616 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/201606-citibike-tripdata.csv", inferSchema='true')

cb716 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/201607-citibike-tripdata.csv", inferSchema='true')

cb816 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/201608-citibike-tripdata.csv", inferSchema='true')

cb916 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/201609-citibike-tripdata.csv", inferSchema='true')

cb1016 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/201610-citibike-tripdata.csv", inferSchema='true')

cb1116 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/201611-citibike-tripdata.csv", inferSchema='true')

cb1216 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/201612-citibike-tripdata.csv", inferSchema='true')

#Select the columns you need

#2015
cb115 = cb115.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')

cb215 = cb215.select(col('start station id'), col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb315 = cb315.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb415 = cb415.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb515 = cb515.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb615 = cb615.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb715 = cb715.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb815 = cb815.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb915 = cb915.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb1015 = cb1015.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb1115 = cb1115.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb1215 = cb1215.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')

#2014
cb114 = cb114.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb214 = cb214.select(col('start station id'), col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb314 = cb314.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb414 = cb414.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb514 = cb514.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb614 = cb614.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb714 = cb714.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb814 = cb814.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb914 = cb914.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb1014 = cb1014.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb1114 = cb1114.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb1214 = cb1214.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')

#2016
cb116 = cb116.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb216 = cb216.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb316 = cb316.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb416 = cb416.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb516 = cb516.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb616 = cb616.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb716 = cb716.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb816 = cb816.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb916 = cb916.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb1016 = cb1016.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb1116 = cb1116.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb1216 = cb1216.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')

#2013
cb713 = cb713.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb813 = cb813.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb913 = cb913.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb1013 = cb1013.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb1113 = cb1113.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')
cb1213 = cb1213.select(col('start station id'),col('start station name'))\
.withColumnRenamed('start station id','sid').withColumnRenamed('start station name','sname')

citibike_13 = cb713.unionAll(cb813).unionAll(cb913).unionAll(cb1013)\
.unionAll(cb1113).unionAll(cb1213)

citibike_14 = cb114.unionAll(cb214)\
.unionAll(cb314).unionAll(cb414).unionAll(cb514).unionAll(cb614)\
.unionAll(cb714).unionAll(cb814).unionAll(cb914).unionAll(cb1014)\
.unionAll(cb1114).unionAll(cb1214)

citibike_15 = cb115.unionAll(cb215).unionAll(cb315).unionAll(cb415).unionAll(cb515)\
.unionAll(cb615).unionAll(cb715).unionAll(cb815).unionAll(cb915)\
.unionAll(cb1015).unionAll(cb1115).unionAll(cb1215)

citibike_16 = cb116.unionAll(cb216).unionAll(cb316).unionAll(cb416).unionAll(cb516)\
.unionAll(cb616).unionAll(cb716).unionAll(cb816).unionAll(cb916).unionAll(cb1016)\
.unionAll(cb1116).unionAll(cb1216)

citibike_data = citibike_13.unionAll(citibike_14).unionAll(citibike_15).unionAll(citibike_16)

citibike_data.createOrReplaceTempView('citibike_data')

top_ten = spark.sql("""SELECT sname, COUNT(sid) as num_trips from citibike_data GROUP BY sid,sname ORDER BY num_trips limit 10""")

top_ten.printSchema()
#top_ten.toPandas().to_csv('top_ten.csv')