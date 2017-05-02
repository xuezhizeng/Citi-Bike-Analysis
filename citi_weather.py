from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import *
import datetime
from pyspark.sql.types import *
import pandas as pd
from pyspark.sql import Row
from datetime import date


#warehouseLocation = "file:/home/ta1302/spark-2.1.0-bin-hadoop2.6/spark-warehouse"
#spark = SparkSession.builder().appName("SparkSessionZipsExample").config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()

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

#weather data
weather = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("/home/ta1302/Citi-Bike-Analysis/data/central_park_weather.csv", inferSchema='true')	

#select the colums we need
#2013
cb713 = cb713.select(cb713.starttime, cb713.bikeid)
cb813 = cb813.select(cb813.starttime, cb813.bikeid)
cb913 = cb913.select(cb913.starttime, cb913.bikeid)
cb1013 = cb1013.select(cb1013.starttime, cb1013.bikeid)
cb1113 = cb1113.select(cb1113.starttime, cb1113.bikeid)
cb1213 = cb1213.select(cb1213.starttime, cb1213.bikeid)

#2014
cb114 = cb114.select(cb114.starttime, cb114.bikeid)
cb214 = cb214.select(cb214.starttime, cb214.bikeid)
cb314 = cb314.select(cb314.starttime, cb314.bikeid)
cb414 = cb414.select(cb414.starttime, cb414.bikeid)
cb514 = cb514.select(cb514.starttime, cb514.bikeid)
cb614 = cb614.select(cb614.starttime, cb614.bikeid)
cb714 = cb714.select(cb714.starttime, cb714.bikeid)
cb814 = cb814.select(cb814.starttime, cb814.bikeid)
cb914 = cb914.select(cb914.starttime, cb914.bikeid)
cb1014 = cb1014.select(cb1014.starttime, cb1014.bikeid)
cb1114 = cb1114.select(cb1114.starttime, cb1114.bikeid)
cb1214 = cb1214.select(cb1214.starttime, cb1214.bikeid)

#2015
cb115 = cb115.select(cb115.starttime, cb115.bikeid)
cb215 = cb215.select(cb215.starttime, cb215.bikeid)
cb315 = cb315.select(cb315.starttime, cb315.bikeid)
cb415 = cb415.select(cb415.starttime, cb415.bikeid)
cb515 = cb515.select(cb515.starttime, cb515.bikeid)
cb615 = cb615.select(cb615.starttime, cb615.bikeid)
cb715 = cb715.select(cb715.starttime, cb715.bikeid)
cb815 = cb815.select(cb815.starttime, cb815.bikeid)
cb915 = cb915.select(cb915.starttime, cb915.bikeid)
cb1015 = cb1015.select(cb1015.starttime, cb1015.bikeid)
cb1115 = cb1115.select(cb1115.starttime, cb1115.bikeid)
cb1215 = cb1215.select(cb1215.starttime, cb1215.bikeid)

#2016
cb116 = cb116.select(cb116.starttime, cb116.bikeid)
cb216 = cb216.select(cb216.starttime, cb216.bikeid)
cb316 = cb316.select(cb316.starttime, cb316.bikeid)
cb416 = cb416.select(cb416.starttime, cb416.bikeid)
cb516 = cb516.select(cb516.starttime, cb516.bikeid)
cb616 = cb616.select(cb616.starttime, cb616.bikeid)
cb716 = cb716.select(cb716.starttime, cb716.bikeid)
cb816 = cb816.select(cb816.starttime, cb816.bikeid)
cb916 = cb916.select(cb916.starttime, cb916.bikeid)
cb1016 = cb1016.withColumnRenamed('Start Time', 'starttime').withColumnRenamed('Bike ID','bikeid').select(col('starttime'),col('bikeid'))
cb1116 = cb1116.withColumnRenamed('Start Time', 'starttime').withColumnRenamed('Bike ID','bikeid').select(col('starttime'),col('bikeid'))
cb1216 = cb1216.withColumnRenamed('Start Time', 'starttime').withColumnRenamed('Bike ID','bikeid').select(col('starttime'),col('bikeid'))

citibike_13 = cb713.unionAll(cb813).unionAll(cb913).unionAll(cb1013)\
.unionAll(cb1113).unionAll(cb1213)

citibike_14_8 = cb114.unionAll(cb214)\
.unionAll(cb314).unionAll(cb414).unionAll(cb514).unionAll(cb614)\
.unionAll(cb714).unionAll(cb814)

citibike_14_9 = cb914.unionAll(cb1014)\
.unionAll(cb1114).unionAll(cb1214)

citibike_15 = cb115.unionAll(cb215).unionAll(cb315).unionAll(cb415).unionAll(cb515)\
.unionAll(cb615).unionAll(cb715).unionAll(cb815).unionAll(cb915)\
.unionAll(cb1015).unionAll(cb1115).unionAll(cb1215)

citibike_16_9 = cb116.unionAll(cb216).unionAll(cb316).unionAll(cb416).unionAll(cb516)\
.unionAll(cb616).unionAll(cb716).unionAll(cb816).unionAll(cb916)

citibike_16_10 = cb1016.unionAll(cb1116).unionAll(cb1216)


#Change the date format for 2013
new_format = 'MM/dd/yyy'
citibike_13 = citibike_13.withColumn('new_format', from_unixtime(unix_timestamp(citibike_13.starttime, 'yyy/MM/dd'), new_format).alias('date'))

citibike_13 = citibike_13.withColumn('timestamp',unix_timestamp('new_format','MM/dd/yyy').cast("double").cast("timestamp"))

citibike_13 = citibike_13.withColumn('day_of_year', dayofyear(col('timestamp'))).drop('starttime', 'new_format', 'timestamp')

#Change the date format for 2014
new_format = 'MM/dd/yyy'

citibike_14_8 = citibike_14_8.withColumn('new_format', from_unixtime(unix_timestamp(citibike_14_8.starttime, 'yyy/MM/dd'), new_format).alias('date'))

citibike_14_9 = citibike_14_9.withColumn('new_format', from_unixtime(unix_timestamp(citibike_14_9.starttime, 'M/d/yyy'), new_format).alias('date'))

citibike_14 = citibike_14_8.unionAll(citibike_14_9)

citibike_14 = citibike_14.withColumn("timestamp", unix_timestamp("new_format", "MM/dd/yyy").cast("double").cast("timestamp"))

citibike_14 = citibike_14.withColumn('day_of_year', dayofyear(col('timestamp'))).drop('starttime', 'new_format', 'timestamp')

#Change the date format for 2015

new_format = 'MM/dd/yyy'

citibike_15 = citibike_15.withColumn('new_format', from_unixtime(unix_timestamp(citibike_15.starttime, 'M/d/yyy'), new_format).alias('date'))

citibike_15 = citibike_15.withColumn("timestamp", unix_timestamp("new_format", "MM/dd/yyy").cast("double").cast("timestamp"))

citibike_15 = citibike_15.withColumn('day_of_year', dayofyear(col('timestamp'))).drop('starttime', 'new_format', 'timestamp')

#Change the date format for 2016

new_format = 'MM/dd/yyy'

citibike_16_9 = citibike_16_9.withColumn('new_format', from_unixtime(unix_timestamp(citibike_16_9.starttime, 'M/d/yyy'), new_format).alias('date'))

citibike_16_10 = citibike_16_10.withColumn('new_format', from_unixtime(unix_timestamp(citibike_16_10.starttime, 'yyy/MM/dd'), new_format).alias('date'))

citibike_16 = citibike_16_9.unionAll(citibike_16_10)

citibike_16 = citibike_16.withColumn("timestamp", unix_timestamp("new_format", "MM/dd/yyy").cast("double").cast("timestamp"))

citibike_16 = citibike_16.withColumn('day_of_year', dayofyear(col('timestamp'))).drop('starttime', 'new_format', 'timestamp')

##Conver to Pandas as Spark Datetime functions are timezone sensitive 
df_weather = weather.toPandas()

#Change date from String to Date Format
df_weather['DATE'] = pd.to_datetime(df_weather['DATE'], format='%Y%m%d')

#Take year wise weather
df_weather_13 = df_weather[(df_weather['DATE']>=date(2013,7,1)) & (df_weather['DATE']<=date(2013,12,31))]

df_weather_14 = df_weather[(df_weather['DATE']>=date(2014,1,1)) & (df_weather['DATE']<=date(2014,12,31))]

df_weather_15 = df_weather[(df_weather['DATE']>=date(2015,1,1)) & (df_weather['DATE']<=date(2015,12,31))]

df_weather_16 = df_weather[(df_weather['DATE']>=date(2016,1,1)) & (df_weather['DATE']<=date(2016,12,31))]


#Get Day of Year for weather data
df_weather_13['day_of_yr'] = df_weather_13.DATE.apply(lambda x: x.dayofyear)
df_weather_14['day_of_yr'] = df_weather_14.DATE.apply(lambda x: x.dayofyear)
df_weather_15['day_of_yr'] = df_weather_15.DATE.apply(lambda x: x.dayofyear)
df_weather_16['day_of_yr'] = df_weather_16.DATE.apply(lambda x: x.dayofyear)

#Create a spark dataframe for Analysis
weather13 = spark.createDataFrame(df_weather_13)
weather14 = spark.createDataFrame(df_weather_14)
weather15 = spark.createDataFrame(df_weather_15)
weather16 = spark.createDataFrame(df_weather_16)

#Define a User Defined Function to get whether it snowed or not
def snowed(x):
    if x > 0:
        return 1
    else:
        return 0
		
#Register the UDF
snowed_or_not = udf(snowed, IntegerType())

#Calculate Average Temperature and Add snow column
weather13 = weather13.withColumn('AVG_T', (weather13.TMAX + weather13.TMIN)/2).withColumn('snowed', snowed_or_not(weather13.SNOW))\
.drop('STATION', 'STATION_NAME', 'DATE','TMAX', 'TMIN')

weather14 = weather14.withColumn('AVG_T', (weather14.TMAX + weather14.TMIN)/2).withColumn('snowed', snowed_or_not(weather14.SNOW))\
.drop('STATION', 'STATION_NAME', 'DATE','TMAX', 'TMIN')

weather15 = weather15.withColumn('AVG_T', (weather15.TMAX + weather15.TMIN)/2).withColumn('snowed', snowed_or_not(weather15.SNOW))\
.drop('STATION', 'STATION_NAME', 'DATE','TMAX', 'TMIN')

weather16 = weather16.withColumn('AVG_T', (weather16.TMAX + weather16.TMIN)/2).withColumn('snowed', snowed_or_not(weather16.SNOW))\
.drop('STATION', 'STATION_NAME', 'DATE','TMAX', 'TMIN')


#Create a temp weather table
weather13.createOrReplaceTempView('weather13')
weather14.createOrReplaceTempView('weather14')
weather15.createOrReplaceTempView('weather15')
weather16.createOrReplaceTempView('weather16')
		
#Create a temp citibike data table
citibike_13.createOrReplaceTempView('citibike13')
citibike_14.createOrReplaceTempView('citibike14')
citibike_15.createOrReplaceTempView('citibike15')
citibike_16.createOrReplaceTempView('citibike16')

# Run SQL queries for Analysis
sqlquery_13 = spark.sql('SELECT day_of_year, count(bikeid) as num_trips FROM citibike13 GROUP BY day_of_year ORDER BY day_of_year')
sqlquery_14 = spark.sql('SELECT day_of_year, count(bikeid) as num_trips FROM citibike14 GROUP BY day_of_year ORDER BY day_of_year')
sqlquery_15 = spark.sql('SELECT day_of_year, count(bikeid) as num_trips FROM citibike15 GROUP BY day_of_year ORDER BY day_of_year')
sqlquery_16 = spark.sql('SELECT day_of_year, count(bikeid) as num_trips FROM citibike16 GROUP BY day_of_year ORDER BY day_of_year')

#Create a temp citibike data table
sqlquery_13.createOrReplaceTempView('citibike13')
sqlquery_14.createOrReplaceTempView('citibike14')
sqlquery_15.createOrReplaceTempView('citibike15')
sqlquery_16.createOrReplaceTempView('citibike16')

#final_table = sqlquery_13.unionAll(sqlquery_14).unionAll(sqlquery_15).unionAll(sqlquery_16)

#final_table.createOrReplaceTempView('final_table')

final_query13 = spark.sql("""SELECT day_of_yr, PRCP, SNWD, AWND, AVG_T, snowed, num_trips FROM citibike13 as c INNER JOIN weather13 as w ON w.day_of_yr =c.day_of_year""")
final_query14 = spark.sql("""SELECT day_of_yr, PRCP, SNWD, AWND, AVG_T, snowed, num_trips FROM citibike14 as c INNER JOIN weather14 as w ON w.day_of_yr =c.day_of_year""")
final_query15 = spark.sql("""SELECT day_of_yr, PRCP, SNWD, AWND, AVG_T, snowed, num_trips FROM citibike15 as c INNER JOIN weather15 as w ON w.day_of_yr =c.day_of_year""")
final_query16 = spark.sql("""SELECT day_of_yr, PRCP, SNWD, AWND, AVG_T, snowed, num_trips FROM citibike16 as c INNER JOIN weather16 as w ON w.day_of_yr =c.day_of_year""")

#Save the Analysis to CSV for further Visualization
final_query13.sort(col('day_of_yr')).toPandas().to_csv('citi_weather13.csv')
final_query14.sort(col('day_of_yr')).toPandas().to_csv('citi_weather14.csv')
final_query15.sort(col('day_of_yr')).toPandas().to_csv('citi_weather15.csv')
final_query16.sort(col('day_of_yr')).toPandas().to_csv('citi_weather16.csv')