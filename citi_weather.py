from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import *
import datetime
from pyspark.sql.types import *
import pandas as pd
from pyspark.sql import Row
from datetime import date

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
	

#2013 Data
cb713 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("2013-07 - Citi Bike trip data.csv", inferSchema='true')	

cb813 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("2013-08 - Citi Bike trip data.csv", inferSchema='true')

cb913 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("2013-09 - Citi Bike trip data.csv", inferSchema='true')

cb1013 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("2013-10 - Citi Bike trip data.csv", inferSchema='true')

cb1113 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("2013-11 - Citi Bike trip data.csv", inferSchema='true')

cb1213 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("2013-12 - Citi Bike trip data.csv", inferSchema='true')	

#2014 Data
cb114 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("citibike/2014-01 - Citi Bike trip data.csv", inferSchema='true')

cb214 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("citibike/2014-02 - Citi Bike trip data.csv", inferSchema='true')

cb314 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("citibike/2014-03 - Citi Bike trip data.csv", inferSchema='true')

cb414 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("citibike/2014-04 - Citi Bike trip data.csv", inferSchema='true')

cb514 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("citibike/2014-05 - Citi Bike trip data.csv", inferSchema='true')

cb614 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("citibike/2014-06 - Citi Bike trip data.csv", inferSchema='true')

cb714 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("citibike/2014-07 - Citi Bike trip data.csv", inferSchema='true')

cb814 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("citibike/2014-08 - Citi Bike trip data.csv", inferSchema='true')

cb914 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("citibike/2014-09 - Citi Bike trip data.csv", inferSchema='true')

cb1014 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("citibike/2014-10 - Citi Bike trip data.csv", inferSchema='true')

cb1114 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("citibike/2014-11 - Citi Bike trip data.csv", inferSchema='true')

cb1214 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("citibike/2014-12 - Citi Bike trip data.csv", inferSchema='true')


#2015 Data	
cb115 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("201501-citibike-tripdata.csv", inferSchema='true')

cb215 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("201502-citibike-tripdata.csv", inferSchema='true')

cb315 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("201503-citibike-tripdata.csv", inferSchema='true')

cb415 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("201504-citibike-tripdata.csv", inferSchema='true')

cb515 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("201505-citibike-tripdata.csv", inferSchema='true')

cb615 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("201506-citibike-tripdata.csv", inferSchema='true')

cb715 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("201507-citibike-tripdata.csv", inferSchema='true')

cb815 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("201508-citibike-tripdata.csv", inferSchema='true')

cb915 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("201509-citibike-tripdata.csv", inferSchema='true')

cb1015 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("201510-citibike-tripdata.csv", inferSchema='true')

cb1115 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("201511-citibike-tripdata.csv", inferSchema='true')

cb1215 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("201512-citibike-tripdata.csv", inferSchema='true')

#2016 Data

cb116 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("201601-citibike-tripdata.csv", inferSchema='true')

cb216 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("201602-citibike-tripdata.csv", inferSchema='true')

cb316 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("201603-citibike-tripdata.csv", inferSchema='true')

cb416 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("201604-citibike-tripdata.csv", inferSchema='true')

cb516 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("201605-citibike-tripdata.csv", inferSchema='true')

cb616 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("201606-citibike-tripdata.csv", inferSchema='true')

cb716 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("201607-citibike-tripdata.csv", inferSchema='true')

cb816 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("201608-citibike-tripdata.csv", inferSchema='true')

cb916 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("201609-citibike-tripdata.csv", inferSchema='true')

cb1016 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("201610-citibike-tripdata.csv", inferSchema='true')

cb1116 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("201611-citibike-tripdata.csv", inferSchema='true')

cb1216 = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("201612-citibike-tripdata.csv", inferSchema='true')

#weather data
weather = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.load("central_park_weather.csv", inferSchema='true')	

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
cb116 = cb115.select(cb116.starttime, cb116.bikeid)
cb216 = cb215.select(cb216.starttime, cb216.bikeid)
cb316 = cb315.select(cb316.starttime, cb316.bikeid)
cb416 = cb415.select(cb416.starttime, cb416.bikeid)
cb516 = cb515.select(cb516.starttime, cb516.bikeid)
cb616 = cb615.select(cb616.starttime, cb616.bikeid)
cb716 = cb715.select(cb716.starttime, cb716.bikeid)
cb816 = cb815.select(cb816.starttime, cb816.bikeid)
cb916 = cb915.select(cb916.starttime, cb916.bikeid)
cb1016 = cb1015.select(cb1016.starttime, cb1016.bikeid)
cb1116 = cb1115.select(cb1116.starttime, cb1116.bikeid)
cb1216 = cb1216.select(cb1216.starttime, cb1216.bikeid)

citibike_data = cb713.unionAll(cb813).unionAll(cb913).unionAll(cb1013)\
.unionAll(cb1113).unionAll(cb1213).unionAll(cb114).unionAll(cb214)\
.unionAll(cb314).unionAll(cb414).unionAll(cb514).unionAll(cb614)\
.unionAll(cb714).unionAll(cb814).unionAll(cb914).unionAll(cb1014)\
.unionAll(cb1114).unionAll(cb1214).unionAll(cb115)\
.unionAll(cb215).unionAll(cb315).unionAll(cb415).unionAll(cb515)\
.unionAll(cb615).unionAll(cb715).unionAll(cb815).unionAll(cb915)\
.unionAll(cb1015).unionAll(cb1115).unionAll(cb1215).unionAll(cb116)\
.unionAll(cb216).unionAll(cb316).unionAll(cb416).unionAll(cb516)\
.unionAll(cb616).unionAll(cb716).unionAll(cb816).unionAll(cb916)\
.unionAll(cb1016).unionAll(cb1116).unionAll(cb1216)

#Change the date format
new_format = 'MM/dd/yyy'
citibike_data = citibike_data.withColumn('new_format', from_unixtime(unix_timestamp(citibike_data.starttime, 'M/d/yyy'), new_format).alias('date'))

#Conver to Pandas as Spark Datetime functions are timezone sensitive
citibike_data_pd = citibike_data.toPandas()

#Change starttime from String to Date Format
citibike_data_pd['new_format'] = pd.to_datetime(citibike_data_pd['new_format'], infer_datetime_format=True)

#Get Day of the Year
citibike_data_pd['day_of_year'] = citibike_data_pd.new_format.apply(lambda x: x.dayofyear)

#Drop the colums you don't need
citibike_data_pd = citibike_data_pd.drop(['new_format'], axis=1)

#Create a spark dataframe for Big Data Analysis
citibike_data = spark.createDataFrame(citibike_data_pd)

##Conver to Pandas as Spark Datetime functions are timezone sensitive 
df_weather = weather.toPandas()

#Change date from String to Date Format
df_weather['DATE'] = pd.to_datetime(df_weather['DATE'], format='%Y%m%d')

#Get Day of Year for weather data
df_weather['day_of_yr'] = df_weather.DATE.apply(lambda x: x.dayofyear)

#Create a spark dataframe for Analysis
weather_df = spark.createDataFrame(df_weather)

#Define a User Defined Function to get whether it snowed or not
def snowed(x):
    if x > 0:
        return 1
    else:
        return 0
		
#Register the UDF
snowed_or_not = udf(snowed, IntegerType())

#Calculate Average Temperature and Add snow column
weather_df = weather_df.withColumn('AVG_T', (weather_df.TMAX + weather_df.TMIN)/2).withColumn('snowed', snowed_or_not(weather_df.SNOW))\
.drop('STATION', 'STATION_NAME', 'DATE','TMAX', 'TMIN')

#Create a temp weather table
weather_df.createOrReplaceTempView('weather_df')		

#Create a temp citibike data table
citibike_data.createOrReplaceTempView('citibike_data')

# Run SQL queries for Analysis
sqlquery = spark.sql('SELECT day_of_year, count(bikeid) as num_trips FROM citibike_data GROUP BY day_of_year')
citibike_data = sqlquery
citibike_data.createOrReplaceTempView('citibike_data')
sqlquery1 = spark.sql("""SELECT num_trips, PRCP, SNWD, AWND, AVG_T, w.snowed FROM weather_df as w INNER JOIN citibike_data as c ON w.day_of_yr =c.day_of_year""")

#Save the Analysis to CSV for further Visualization
sqlquery1.toPandas().to_csv('citi_weather.csv')