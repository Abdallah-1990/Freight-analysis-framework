from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.rdd import portable_hash
from pyspark.statcounter import StatCounter
from pyspark.sql.types import StringType,IntegerType,FloatType
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql.functions import to_timestamp, current_timestamp, col,expr,unix_timestamp,round,when,hour
import os
import json
#%matplotlib inline
from datetime import datetime
from matplotlib import pyplot as plt
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import *
from pyspark.ml.feature import StringIndexer,OneHotEncoder,VectorAssembler, VectorSlicer
from pyspark.ml.feature import OneHotEncoder, OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.linalg import Vectors
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit , CrossValidator
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import IndexToString

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("FAF").master("spark://abdullah:7077").config("spark.driver.host", "192.168.0.110").getOrCreate()
#.config("spark.driver.memory", "4g").config("spark.driver.cores", "2")
sc=spark.sparkContext
spark

#read dataSet 2012
df_2012_domistic = spark.read.csv("file:///home/abdullah/Downloads/df_2012_domistic.csv",header=True, inferSchema=True)

#read dataSet 2013
df_2013_domistic = spark.read.csv("file:///home/abdullah/Downloads/df_2013_domistic.csv",header=True, inferSchema=True)

#read dataSet 2014
df_2014_domistic = spark.read.csv("file:///home/abdullah/Downloads/df_2014_domistic.csv",header=True, inferSchema=True)


#read dataSet 2015
df_2015_domistic = spark.read.csv("file:///home/abdullah/Downloads/df_2015_domistic.csv",header=True, inferSchema=True)

#read dataSet 2016
df_2016_domistic = spark.read.csv("file:///home/abdullah/Downloads/df_2016_domistic.csv",header=True, inferSchema=True)

#read dataSet 2017
df_2017_domistic = spark.read.csv("file:///home/abdullah/Downloads/df_2017_domistic.csv",header=True, inferSchema=True)

#read dataSet 2018
df_2018_domistic = spark.read.csv("file:///home/abdullah/Downloads/df_2018_domistic.csv",header=True, inferSchema=True)


#Inner join between six dataSets with conditions of origin , destination , mode of transportation , commodity type and the distance

cond_13=[df_2012_domistic.dms_orig == df_2013_domistic.dms_orig_13,df_2012_domistic.dms_dest == df_2013_domistic.dms_dest_13,df_2012_domistic.dms_mode == df_2013_domistic.dms_mode_13,df_2012_domistic.sctg2 == df_2013_domistic.sctg2_13,df_2012_domistic.wgt_dist == df_2013_domistic.wgt_dist_13]

df_12_13 =df_2012_domistic.join(df_2013_domistic,cond_13 ).drop("dms_orig_13","dms_dest_13","dms_mode_13","sctg2_13","wgt_dist_13")


cond_14=[df_12_13.dms_orig == df_2014_domistic.dms_orig_14,df_12_13.dms_dest == df_2014_domistic.dms_dest_14,df_12_13.dms_mode == df_2014_domistic.dms_mode_14,df_12_13.sctg2 == df_2014_domistic.sctg2_14,df_12_13.wgt_dist == df_2014_domistic.wgt_dist_14]

df_13_14 =df_12_13.join(df_2014_domistic,cond_14 ).drop("dms_orig_14","dms_dest_14","dms_mode_14","sctg2_14","wgt_dist_14")


cond_15=[df_13_14.dms_orig == df_2015_domistic.dms_orig_15,df_13_14.dms_dest == df_2015_domistic.dms_dest_15,df_13_14.dms_mode == df_2015_domistic.dms_mode_15,df_13_14.sctg2 == df_2015_domistic.sctg2_15,df_13_14.wgt_dist == df_2015_domistic.wgt_dist_15]

df_13_14_15 =df_13_14.join(df_2015_domistic,cond_15 ).drop("dms_orig_15","dms_dest_15","dms_mode_15","sctg2_15","wgt_dist_15")


cond_16=[df_13_14_15.dms_orig == df_2016_domistic.dms_orig_16,df_13_14_15.dms_dest == df_2016_domistic.dms_dest_16,df_13_14_15.dms_mode == df_2016_domistic.dms_mode_16,df_13_14_15.sctg2 == df_2016_domistic.sctg2_16,df_13_14_15.wgt_dist == df_2016_domistic.wgt_dist_16]

df_13_14_15_16 =df_13_14_15.join(df_2016_domistic,cond_16 ).drop("dms_orig_16","dms_dest_16","dms_mode_16","sctg2_16","wgt_dist_16")


cond_17=[df_13_14_15_16.dms_orig == df_2017_domistic.dms_orig_17,df_13_14_15_16.dms_dest == df_2017_domistic.dms_dest_17,df_13_14_15_16.dms_mode == df_2017_domistic.dms_mode_17,df_13_14_15_16.sctg2 == df_2017_domistic.sctg2_17,df_13_14_15_16.wgt_dist == df_2017_domistic.wgt_dist_17]

df_13_14_15_16_17 =df_13_14_15_16.join(df_2017_domistic,cond_17 ).drop("dms_orig_17","dms_dest_17","dms_mode_17","sctg2_17","wgt_dist_17")


cond_18=[df_13_14_15_16_17.dms_orig == df_2018_domistic.dms_orig_18,df_13_14_15_16_17.dms_dest == df_2018_domistic.dms_dest_18,df_13_14_15_16_17.dms_mode == df_2018_domistic.dms_mode_18,df_13_14_15_16_17.sctg2 == df_2018_domistic.sctg2_18,df_13_14_15_16_17.wgt_dist == df_2018_domistic.wgt_dist_18]

df_13_14_15_16_17_18 =df_13_14_15_16_17.join(df_2018_domistic,cond_18 ).drop("dms_orig_18","dms_dest_18","dms_mode_18","sctg2_18","wgt_dist_18")

#calculation of the growth rate for each commodity in each region-to-region from 2013 to 2018
df_with_growth_rate=df_13_14_15_16_17_18.withColumn("tons_GrowthRate_12_13",((col("tons_2013") - col("tons_2012")) / (col("tons_2012"))) * 100 )
df_with_growth_rate=df_with_growth_rate.withColumn("tons_GrowthRate_13_14",((col("tons_2014") - col("tons_2013")) / (col("tons_2013"))) * 100 )
df_with_growth_rate=df_with_growth_rate.withColumn("tons_GrowthRate_14_15",((col("tons_2015") - col("tons_2014")) / (col("tons_2014"))) * 100 )
df_with_growth_rate=df_with_growth_rate.withColumn("tons_GrowthRate_15_16",((col("tons_2016") - col("tons_2015")) / (col("tons_2015"))) * 100 )
df_with_growth_rate=df_with_growth_rate.withColumn("tons_GrowthRate_16_17",((col("tons_2017") - col("tons_2016")) / (col("tons_2016"))) * 100 )
df_with_growth_rate=df_with_growth_rate.withColumn("tons_GrowthRate_17_18",((col("tons_2018") - col("tons_2017")) / (col("tons_2017"))) * 100 )

#Without null valuas
df_with_growth_rate=df_with_growth_rate.where(col("tons_GrowthRate_13_14").isNotNull())

#calculation of the average for the growth rate from 2013 to 2018
df_with_growth_rate=df_with_growth_rate.withColumn("Avg_tons_GrowthRate_12_18",((col("tons_GrowthRate_12_13") + col("tons_GrowthRate_13_14") + col("tons_GrowthRate_14_15") + col("tons_GrowthRate_15_16") + col("tons_GrowthRate_16_17") + col("tons_GrowthRate_17_18")) /6))

#This is the growth rate for goods tons from 2013 to each year until 2018 for each commodities in specific region-to-region and after 2018 I used the avaerage of growth rate from 2012 to 2018 and added it for each year as a year growth rate to 2025
df_with_growth_rate=df_with_growth_rate.withColumn("tons_GrowthRate_12_14", col("tons_GrowthRate_12_13") + col("tons_GrowthRate_13_14"))
df_with_growth_rate=df_with_growth_rate.withColumn("tons_GrowthRate_12_15", col("tons_GrowthRate_12_14") + col("tons_GrowthRate_14_15"))
df_with_growth_rate=df_with_growth_rate.withColumn("tons_GrowthRate_12_16", col("tons_GrowthRate_12_15") + col("tons_GrowthRate_15_16"))
df_with_growth_rate=df_with_growth_rate.withColumn("tons_GrowthRate_12_17", col("tons_GrowthRate_12_16") + col("tons_GrowthRate_16_17"))
df_with_growth_rate=df_with_growth_rate.withColumn("tons_GrowthRate_12_18", col("tons_GrowthRate_12_17") + col("tons_GrowthRate_17_18"))
df_with_growth_rate=df_with_growth_rate.withColumn("tons_GrowthRate_12_19", col("tons_GrowthRate_12_18") + col("Avg_tons_GrowthRate_12_18"))
df_with_growth_rate=df_with_growth_rate.withColumn("tons_GrowthRate_12_20", col("tons_GrowthRate_12_19") + col("Avg_tons_GrowthRate_12_18"))
df_with_growth_rate=df_with_growth_rate.withColumn("tons_GrowthRate_12_21", col("tons_GrowthRate_12_20") + col("Avg_tons_GrowthRate_12_18"))
df_with_growth_rate=df_with_growth_rate.withColumn("tons_GrowthRate_12_22", col("tons_GrowthRate_12_21") + col("Avg_tons_GrowthRate_12_18"))
df_with_growth_rate=df_with_growth_rate.withColumn("tons_GrowthRate_12_23", col("tons_GrowthRate_12_22") + col("Avg_tons_GrowthRate_12_18"))
df_with_growth_rate=df_with_growth_rate.withColumn("tons_GrowthRate_12_24", col("tons_GrowthRate_12_23") + col("Avg_tons_GrowthRate_12_18"))
#df_with_growth_rate=df_with_growth_rate.withColumn("tons_GrowthRate_12_25", col("tons_GrowthRate_12_24") + col("Avg_tons_GrowthRate_12_18"))

#Extract the tables with columns I need to use for training and create regression model with creating year column
from pyspark.sql.functions import lit
df_ton_2013=df_with_growth_rate.select("dms_orig","dms_dest","dms_mode","sctg2","wgt_dist",col("tons_GrowthRate_12_13").alias("tons_GrowthRate"),col("tons_2013").alias("tons")).withColumn("year", lit(2013))
df_ton_2014=df_with_growth_rate.select("dms_orig","dms_dest","dms_mode","sctg2","wgt_dist",col("tons_GrowthRate_12_14").alias("tons_GrowthRate"),col("tons_2014").alias("tons")).withColumn("year", lit(2014))
df_ton_2015=df_with_growth_rate.select("dms_orig","dms_dest","dms_mode","sctg2","wgt_dist",col("tons_GrowthRate_12_15").alias("tons_GrowthRate"),col("tons_2015").alias("tons")).withColumn("year", lit(2015))
df_ton_2016=df_with_growth_rate.select("dms_orig","dms_dest","dms_mode","sctg2","wgt_dist",col("tons_GrowthRate_12_16").alias("tons_GrowthRate"),col("tons_2016").alias("tons")).withColumn("year", lit(2016))
df_ton_2017=df_with_growth_rate.select("dms_orig","dms_dest","dms_mode","sctg2","wgt_dist",col("tons_GrowthRate_12_17").alias("tons_GrowthRate"),col("tons_2017").alias("tons")).withColumn("year", lit(2017))
df_ton_2018=df_with_growth_rate.select("dms_orig","dms_dest","dms_mode","sctg2","wgt_dist",col("tons_GrowthRate_12_18").alias("tons_GrowthRate"),col("tons_2018").alias("tons")).withColumn("year", lit(2018))

#Union the datasets in one data
df_tons_train = df_ton_2013.union(df_ton_2014)
df_tons_train = df_tons_train.union(df_ton_2015)
df_tons_train = df_tons_train.union(df_ton_2016)
df_tons_train = df_tons_train.union(df_ton_2017)
df_tons_train = df_tons_train.union(df_ton_2018)

dms_orig_Numbers_df=df_tons_train.groupBy("dms_orig").agg(count("*").alias("dms_orig_Numbers")).select('dms_orig')
dms_orig_array = [int(row['dms_orig']) for row in dms_orig_Numbers_df.collect()]

chunks = [dms_orig_array[x:x+6] for x in range(0, len(dms_orig_array), 6)]

#Extract the tables with columns I need to predict the tons for it
#df_ton_2019=df_with_growth_rate.select("dms_orig","dms_dest","dms_mode","sctg2","wgt_dist",col("tons_GrowthRate_12_19").alias("tons_GrowthRate")).withColumn("year", lit(2019))
#df_ton_2020=df_with_growth_rate.select("dms_orig","dms_dest","dms_mode","sctg2","wgt_dist",col("tons_GrowthRate_12_20").alias("tons_GrowthRate")).withColumn("year", lit(2020))
#df_ton_2021=df_with_growth_rate.select("dms_orig","dms_dest","dms_mode","sctg2","wgt_dist",col("tons_GrowthRate_12_21").alias("tons_GrowthRate")).withColumn("year", lit(2021))
#df_ton_2022=df_with_growth_rate.select("dms_orig","dms_dest","dms_mode","sctg2","wgt_dist",col("tons_GrowthRate_12_22").alias("tons_GrowthRate")).withColumn("year", lit(2022))
#df_ton_2023=df_with_growth_rate.select("dms_orig","dms_dest","dms_mode","sctg2","wgt_dist",col("tons_GrowthRate_12_23").alias("tons_GrowthRate")).withColumn("year", lit(2023))
df_ton_2024=df_with_growth_rate.select("dms_orig","dms_dest","dms_mode","sctg2","wgt_dist",col("tons_GrowthRate_12_24").alias("tons_GrowthRate")).withColumn("year", lit(2024))
#df_ton_2025=df_with_growth_rate.select("dms_orig","dms_dest","dms_mode","sctg2","wgt_dist",col("tons_GrowthRate_12_25").alias("tons_GrowthRate")).withColumn("year", lit(2025))


#Union the datasets in one data
#df_tons_predicted = df_ton_2019.union(df_ton_2020)
#df_tons_predicted = df_tons_predicted.union(df_ton_2021)
#df_tons_predicted = df_tons_predicted.union(df_ton_2022)
#df_tons_predicted = df_tons_predicted.union(df_ton_2023)
#df_tons_predicted = df_tons_predicted.union(df_ton_2024)
#df_tons_predicted = df_tons_predicted.union(df_ton_2025)

df_tons_train_1=df_tons_train.filter(col('dms_orig').isin(chunks[0]))
df_tons_train_2=df_tons_train.filter(col('dms_orig').isin(chunks[1]))
df_tons_train_3=df_tons_train.filter(col('dms_orig').isin(chunks[2]))
df_tons_train_4=df_tons_train.filter(col('dms_orig').isin(chunks[3]))
df_tons_train_5=df_tons_train.filter(col('dms_orig').isin(chunks[4]))
df_tons_train_6=df_tons_train.filter(col('dms_orig').isin(chunks[5]))
df_tons_train_7=df_tons_train.filter(col('dms_orig').isin(chunks[6]))
df_tons_train_8=df_tons_train.filter(col('dms_orig').isin(chunks[7]))
df_tons_train_9=df_tons_train.filter(col('dms_orig').isin(chunks[8]))
df_tons_train_10=df_tons_train.filter(col('dms_orig').isin(chunks[9]))
df_tons_train_11=df_tons_train.filter(col('dms_orig').isin(chunks[10]))
df_tons_train_12=df_tons_train.filter(col('dms_orig').isin(chunks[11]))
df_tons_train_13=df_tons_train.filter(col('dms_orig').isin(chunks[12]))
df_tons_train_14=df_tons_train.filter(col('dms_orig').isin(chunks[13]))
df_tons_train_15=df_tons_train.filter(col('dms_orig').isin(chunks[14]))
df_tons_train_16=df_tons_train.filter(col('dms_orig').isin(chunks[15]))
df_tons_train_17=df_tons_train.filter(col('dms_orig').isin(chunks[16]))
df_tons_train_18=df_tons_train.filter(col('dms_orig').isin(chunks[17]))
df_tons_train_19=df_tons_train.filter(col('dms_orig').isin(chunks[18]))
df_tons_train_20=df_tons_train.filter(col('dms_orig').isin(chunks[19]))
df_tons_train_21=df_tons_train.filter(col('dms_orig').isin(chunks[20]))
df_tons_train_22=df_tons_train.filter(col('dms_orig').isin(chunks[21]))

df_tons_predicted=df_ton_2024

df_tons_predicted_1=df_tons_predicted.filter(col('dms_orig').isin(chunks[0]))
df_tons_predicted_2=df_tons_predicted.filter(col('dms_orig').isin(chunks[1]))
df_tons_predicted_3=df_tons_predicted.filter(col('dms_orig').isin(chunks[2]))
df_tons_predicted_4=df_tons_predicted.filter(col('dms_orig').isin(chunks[3]))
df_tons_predicted_5=df_tons_predicted.filter(col('dms_orig').isin(chunks[4]))
df_tons_predicted_6=df_tons_predicted.filter(col('dms_orig').isin(chunks[5]))
df_tons_predicted_7=df_tons_predicted.filter(col('dms_orig').isin(chunks[6]))
df_tons_predicted_8=df_tons_predicted.filter(col('dms_orig').isin(chunks[7]))
df_tons_predicted_9=df_tons_predicted.filter(col('dms_orig').isin(chunks[8]))
df_tons_predicted_10=df_tons_predicted.filter(col('dms_orig').isin(chunks[9]))
df_tons_predicted_11=df_tons_predicted.filter(col('dms_orig').isin(chunks[10]))
df_tons_predicted_12=df_tons_predicted.filter(col('dms_orig').isin(chunks[11]))
df_tons_predicted_13=df_tons_predicted.filter(col('dms_orig').isin(chunks[12]))
df_tons_predicted_14=df_tons_predicted.filter(col('dms_orig').isin(chunks[13]))
df_tons_predicted_15=df_tons_predicted.filter(col('dms_orig').isin(chunks[14]))
df_tons_predicted_16=df_tons_predicted.filter(col('dms_orig').isin(chunks[15]))
df_tons_predicted_17=df_tons_predicted.filter(col('dms_orig').isin(chunks[16]))
df_tons_predicted_18=df_tons_predicted.filter(col('dms_orig').isin(chunks[17]))
df_tons_predicted_19=df_tons_predicted.filter(col('dms_orig').isin(chunks[18]))
df_tons_predicted_20=df_tons_predicted.filter(col('dms_orig').isin(chunks[19]))
df_tons_predicted_21=df_tons_predicted.filter(col('dms_orig').isin(chunks[20]))
df_tons_predicted_22=df_tons_predicted.filter(col('dms_orig').isin(chunks[21]))

#dbutils.fs.rm("/FileStore/df",True)


df_tons_train_1.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_train_1.csv")
df_tons_train_2.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_train_2.csv")
df_tons_train_3.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_train_3.csv")
df_tons_train_4.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_train_4.csv")
df_tons_train_5.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_train_5.csv")
df_tons_train_6.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_train_6.csv")
df_tons_train_7.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_train_7.csv")
df_tons_train_8.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_train_8.csv")
df_tons_train_9.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_train_9.csv")
df_tons_train_10.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_train_10.csv")
df_tons_train_11.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_train_11.csv")
df_tons_train_12.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_train_12.csv")
df_tons_train_13.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_train_13.csv")
df_tons_train_14.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_train_14.csv")
df_tons_train_15.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_train_15.csv")
df_tons_train_16.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_train_16.csv")
df_tons_train_17.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_train_17.csv")
df_tons_train_18.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_train_18.csv")
df_tons_train_19.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_train_19.csv")
df_tons_train_20.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_train_20.csv")
df_tons_train_21.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_train_21.csv")
df_tons_train_22.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_train_22.csv")

df_tons_predicted_1.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_predicted_1.csv")
df_tons_predicted_2.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_predicted_2.csv")
df_tons_predicted_3.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_predicted_3.csv")
df_tons_predicted_4.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_predicted_4.csv")
df_tons_predicted_5.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_predicted_5.csv")
df_tons_predicted_6.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_predicted_6.csv")
df_tons_predicted_7.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_predicted_7.csv")
df_tons_predicted_8.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_predicted_8.csv")
df_tons_predicted_9.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_predicted_9.csv")
df_tons_predicted_10.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_predicted_10.csv")
df_tons_predicted_11.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_predicted_11.csv")
df_tons_predicted_12.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_predicted_12.csv")
df_tons_predicted_13.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_predicted_13.csv")
df_tons_predicted_14.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_predicted_14.csv")
df_tons_predicted_15.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_predicted_15.csv")
df_tons_predicted_16.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_predicted_16.csv")
df_tons_predicted_17.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_predicted_17.csv")
df_tons_predicted_18.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_predicted_18.csv")
df_tons_predicted_19.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_predicted_19.csv")
df_tons_predicted_20.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_predicted_20.csv")
df_tons_predicted_21.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_predicted_21.csv")
df_tons_predicted_22.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_predicted_22.csv")

df_tons_train_1 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_train_1.csv",header=True, inferSchema=True)
df_tons_predicted_1 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_predicted_1.csv",header=True, inferSchema=True)

df_tons_train_2 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_train_2.csv",header=True, inferSchema=True)
df_tons_predicted_2 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_predicted_2.csv",header=True, inferSchema=True)

df_tons_train_3 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_train_3.csv",header=True, inferSchema=True)
df_tons_predicted_3 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_predicted_3.csv",header=True, inferSchema=True)

df_tons_train_4 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_train_4.csv",header=True, inferSchema=True)
df_tons_predicted_4 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_predicted_4.csv",header=True, inferSchema=True)

df_tons_train_5 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_train_5.csv",header=True, inferSchema=True)
df_tons_predicted_5 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_predicted_5.csv",header=True, inferSchema=True)

df_tons_train_6 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_train_6.csv",header=True, inferSchema=True)
df_tons_predicted_6 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_predicted_6.csv",header=True, inferSchema=True)

df_tons_train_7 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_train_7.csv",header=True, inferSchema=True)
df_tons_predicted_7 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_predicted_7.csv",header=True, inferSchema=True)

df_tons_train_8 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_train_8.csv",header=True, inferSchema=True)
df_tons_predicted_8 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_predicted_8.csv",header=True, inferSchema=True)

df_tons_train_9 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_train_9.csv",header=True, inferSchema=True)
df_tons_predicted_9 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_predicted_9.csv",header=True, inferSchema=True)

df_tons_train_10 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_train_10.csv",header=True, inferSchema=True)
df_tons_predicted_10= spark.read.csv("file:///home/abdullah/Downloads/df_tons_predicted_10.csv",header=True, inferSchema=True)

df_tons_train_11 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_train_11.csv",header=True, inferSchema=True)
df_tons_predicted_11 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_predicted_11.csv",header=True, inferSchema=True)

df_tons_train_12 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_train_12.csv",header=True, inferSchema=True)
df_tons_predicted_12 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_predicted_12.csv",header=True, inferSchema=True)

df_tons_train_13 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_train_13.csv",header=True, inferSchema=True)
df_tons_predicted_13 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_predicted_13.csv",header=True, inferSchema=True)

df_tons_train_14 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_train_14.csv",header=True, inferSchema=True)
df_tons_predicted_14 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_predicted_14.csv",header=True, inferSchema=True)

df_tons_train_15 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_train_15.csv",header=True, inferSchema=True)
df_tons_predicted_15 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_predicted_15.csv",header=True, inferSchema=True)

df_tons_train_16 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_train_16.csv",header=True, inferSchema=True)
df_tons_predicted_16 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_predicted_16.csv",header=True, inferSchema=True)

df_tons_train_17 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_train_17.csv",header=True, inferSchema=True)
df_tons_predicted_17 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_predicted_17.csv",header=True, inferSchema=True)

df_tons_train_18 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_train_18.csv",header=True, inferSchema=True)
df_tons_predicted_18 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_predicted_18.csv",header=True, inferSchema=True)

df_tons_train_19 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_train_19.csv",header=True, inferSchema=True)
df_tons_predicted_19 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_predicted_19.csv",header=True, inferSchema=True)

df_tons_train_20 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_train_20.csv",header=True, inferSchema=True)
df_tons_predicted_20 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_predicted_20.csv",header=True, inferSchema=True)

df_tons_train_21 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_train_21.csv",header=True, inferSchema=True)
df_tons_predicted_21 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_predicted_21.csv",header=True, inferSchema=True)

df_tons_train_22 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_train_22.csv",header=True, inferSchema=True)
df_tons_predicted_22 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_predicted_22.csv",header=True, inferSchema=True)


dict_1 = {11 :"Birmingham-Hoover-Talladega, AL CFS Area" , 12 : "Mobile-Daphne-Fairhope, AL CFS Area" , 19 : "Remainder of Alabama" , 20 : "Alaska" , 41 : "Phoenix-Mesa-Glendale, AZ CFS Area" , 42 : "Tucson-Nogales, AZ CFS Area" , 49 : "Remainder of Arizona" , 50 : "Remainder of Arkansas" , 61 : "Los Angeles-Long Beach, CA CFS Area" , 62 : "Sacramento-Roseville, CA CFS Area" , 63 : "San Diego-Carlsbad-San Marcos, CA CFS Area" , 64 : "San Jose-San Francisco-Oakland, CA CFS Area" , 65 : "Fresno-Madera, CA CFS Area" , 69 : "Remainder of California" , 81 : "Denver-Aurora, CO CFS Area" , 89 : "Remainder of Colorado" , 91 : "Hartford-West Hartford-East Hartford, CT CFS Area" , 92 : "New York-Newark, NY-NJ-CT-PA CFS Area (CT Part)" , 99 : "Remainder of Connecticut" , 101 : "Philadelphia-Reading-Camden, PA-NJ-DE-MD CFS Area (DE Part)" , 109 : "Remainder of Delaware" , 111 : "Washington-Arlington-Alexandria, DC-VA-MD-WV CFS Area (DC Part)" , 121 : "Jacksonville-St. Marys-Palatka, FL-GA CFS Area (FL Part)" , 122 : "Miami-Fort Lauderdale-Port St. Lucie, FL CFS Area" , 123 : "Orlando-Deltona-Daytona Beach, FL CFS Area" , 124 : "Tampa-St. Petersburg-Clearwater, FL CFS Area" , 129 : "Remainder of Florida" , 131 : "Atlanta-Athens-Clarke County-Sandy Springs, GA CFS Area" , 132 : "Savannah-Hinesville-Statesboro, GA CFS Area" , 139 : "Remainder of Georgia" , 151 : "Urban Honolulu, HI CFS Area" , 159 : "Remainder of Hawaii" , 160 : "Idaho" , 171 : "Chicago-Naperville, IL-IN-WI CFS Area (IL Part)" , 172 : "St. Louis-St. Charles-Farmington, MO-IL CFS Area(IL Part)" , 179 : "Remainder of Illinois" , 181 : "Chicago-Naperville, IL-IN-WI CFS Area (IN Part)" , 182 : "Indianapolis-Carmel-Muncie, IN CFS Area" , 183 : "Fort Wayne-Huntington-Auburn, IN CFS Area" , 189 : "Remainder of Indiana" , 190 : "Remainder of Iowa" , 201 : "Kansas City-Overland Park-Kansas City, MO-KS CFS Area (KS Part)" , 202 : "Wichita-Arkansas City-Winfield, KS CFS Area" , 209 : "Remainder of Kansas" , 211 : "Cincinnati-Wilmington-Maysville, OH-KY-IN CFS Area (KY Part)" , 212 : "Louisville/Jefferson County-Elizabethtown-Madison, KY-IN CFS Area (KY Part)" , 219 : "Remainder of Kentucky" , 221 : "Baton Rouge, LA CFS Area" , 222 : "Lake Charles, LA CFS Area" , 223 : "New Orleans-Metairie-Hammond, LA-MS CFS Area(LA Part)" , 229 : "Remainder of Louisiana" , 230 : "Remainder of Maine" , 241 : "Baltimore-Columbia-Towson, MD CFS Area" , 242 : "Washington-Arlington-Alexandria, DC-VA-MD-WV CFS Area (MD Part)" , 249 : "Remainder of Maryland" , 251 : "Boston-Worcester-Providence, MA-RI-NH-CT CFS Area (MA Part)" , 259 : "Remainder of Massachusetts" , 261 : "Detroit-Warren-Ann Arbor, MI CFS Area" , 262 : "Grand Rapids-Wyoming-Muskegon, MI CFS Area" , 269 : "Remainder of Michigan" , 271 : "Minneapolis-St. Paul, MN-WI CFS Area (MN Part)" , 279 : "Remainder of Minnesota" , 280 : "Remainder of Mississippi" , 291 : "Kansas City-Overland Park-Kansas City, MO-KS CFS Area (MO Part)" , 292 : "St. Louis-St. Charles-Farmington, MO-IL CFS Area(MO Part)" , 299 : "Remainder of Missouri" , 300 : "Remainder of Montana" , 311 : "Omaha-Council Bluffs-Fremont, NE-IA CFS Area(NE Part)" , 319 : "Remainder of Nebraska" , 321 : "Las Vegas-Henderson, NV-AZ CFS Area (NV Part)" , 329 : "Remainder of Nevada" , 331 : "Boston-Worcester-Providence, MA-RI-NH-CT CFS Area (NH Part)" , 339 : "Remainder of New Hampshire" , 341 : "New York-Newark, NY-NJ-CT-PA CFS Area (NJ Part)" , 342 : "Philadelphia-Reading-Camden, PA-NJ-DE-MD CFS Area (NJ Part)" , 350 : "Remainder of New Mexico" , 361 : "Albany-Schenectady, NY CFS Area" , 362 : "Buffalo-Cheektowaga, NY CFS Area" , 363 : "New York-Newark, NY-NJ-CT-PA CFS Area (NYPart)" , 364 : "Rochester-Batavia-Seneca Falls, NY CFS Area" , 369 : "Remainder of New York" , 371 : "Charlotte-Concord, NC-SC CFS Area (NC Part)" , 372 : "Greensboro-Winston-Salem-High Point, NC CFS Area" , 373 : "Raleigh-Durham-Chapel Hill, NC CFS Area" , 379 : "Remainder of North Carolina" , 380 : "Remainder of North Dakota" , 391 : "Cincinnati-Wilmington-Maysville, OH-KY-IN CFS Area (OH Part)" , 392 : "Cleveland-Akron-Canton, OH CFS Area" , 393 : "Columbus-Marion-Zanesville, OH CFS Area" , 394 : "Dayton-Springfield-Sidney, OH CFS Area" , 399 : "Remainder of Ohio" , 401 : "Oklahoma City-Shawnee, OK CFS Area" , 402 : "Tulsa-Muskogee-Bartlesville, OK CFS Area" , 409 : "Remainder of Oklahoma" , 411 : "Portland-Vancouver-Salem, OR-WA CFS Area (OR Part)" , 419 : "Remainder of Oregon" , 421 : "Philadelphia-Reading-Camden, PA-NJ-DE-MD CFS Area (PA Part)" , 422 : "Pittsburgh-New Castle-Weirton, PA-OH-WV CFS Area (PA Part)" , 423 : "New York-Newark, NY-NJ-CT-PA CFS Area (PA Part)" , 429 : "Remainder of Pennsylvania" , 441 : "Boston-Worcester-Providence, MA-RI-NH-CT CFS Area (RI Part)" , 451 : "Charleston-North Charleston-Summerville, SC CFS Area" , 452 : "Greenville-Spartanburg-Anderson, SC CFS Area" , 459 : "Remainder of South Carolina" , 460 : "Remainder of South Dakota" , 471 : "Memphis, TN-MS-AR CFS Area (TN Part)" , 472 : "Nashville-Davidson-Murfreesboro, TN CFS Area" , 473 : "Knoxville-Morristown-Sevierville, TN CFS Area" , 479 : "Remainder of Tennessee" , 481 : "Austin-Round Rock, TX CFS Area" , 482 : "Beaumont-Port Arthur, TX CFS Area" , 483 : "Corpus Christi-Kingsville-Alice, TX CFS Area" , 484 : "Dallas-Fort Worth, TX-OK CFS Area (TX Part)" , 485 : "El Paso-Las Cruces, TX-NM CFS Area (TX Part)" , 486 : "Houston-The Woodlands, TX CFS Area" , 487 : "Laredo, TX CFS Area" , 488 : "San Antonio-New Braunfels, TX CFS Area" , 489 : "Remainder of Texas" , 491 : "Salt Lake City-Provo-Orem, UT CFS Area" , 499 : "Remainder of Utah" , 500 : "Remainder of Vermont" , 511 : "Richmond, VA CFS Area" , 512 : "Virginia Beach-Norfolk, VA-NC CFS Area (VA Part)" , 513 : "Washington-Arlington-Alexandria, DC-VA-MD-WV CFS Area (VA Part)" , 519 : "Remainder of Virginia" , 531 : "Seattle-Tacoma, WA CFS Area" , 532 : "Portland-Vancouver-Salem, OR-WA CFS Area (WA Part)" , 539 : "Remainder of Washington" , 540 : "Remainder of West Virginia" , 551 : "Milwaukee-Racine-Waukesha, WI CFS Area" , 559 : "Remainder of Wisconsin" , 560 : "Remainder of Wyoming"}

df_12_18=df_with_growth_rate.select("dms_orig","dms_dest","dms_mode","sctg2","wgt_dist","tons_2012","tons_2013","tons_2014","tons_2015","tons_2016","tons_2017","tons_2018")

user_func =  udf(lambda x: dict_1.get(x), StringType())
#user_func_1 =  udf(lambda x: dict_2.get(x), StringType())

df_12_18 = df_12_18.withColumn('dms_orig',user_func(df_12_18.dms_orig))
df_12_18 = df_12_18.withColumn('dms_dest',user_func(df_12_18.dms_dest))

df_tons_train_1 = df_tons_train_1.withColumn('dms_orig',user_func(df_tons_train_1.dms_orig))
df_tons_train_1 = df_tons_train_1.withColumn('dms_dest',user_func(df_tons_train_1.dms_dest))

df_tons_predicted_1 = df_tons_predicted_1.withColumn('dms_orig',user_func(df_tons_predicted_1.dms_orig))
df_tons_predicted_1 = df_tons_predicted_1.withColumn('dms_dest',user_func(df_tons_predicted_1.dms_dest))

df_tons_train_2 = df_tons_train_2.withColumn('dms_orig',user_func(df_tons_train_2.dms_orig))
df_tons_train_2 = df_tons_train_2.withColumn('dms_dest',user_func(df_tons_train_2.dms_dest))

df_tons_predicted_2 = df_tons_predicted_2.withColumn('dms_orig',user_func(df_tons_predicted_2.dms_orig))
df_tons_predicted_2 = df_tons_predicted_2.withColumn('dms_dest',user_func(df_tons_predicted_2.dms_dest))

df_tons_train_3 = df_tons_train_3.withColumn('dms_orig',user_func(df_tons_train_3.dms_orig))
df_tons_train_3 = df_tons_train_3.withColumn('dms_dest',user_func(df_tons_train_3.dms_dest))

df_tons_predicted_3 = df_tons_predicted_3.withColumn('dms_orig',user_func(df_tons_predicted_3.dms_orig))
df_tons_predicted_3 = df_tons_predicted_3.withColumn('dms_dest',user_func(df_tons_predicted_3.dms_dest))

df_tons_train_4 = df_tons_train_4.withColumn('dms_orig',user_func(df_tons_train_4.dms_orig))
df_tons_train_4 = df_tons_train_4.withColumn('dms_dest',user_func(df_tons_train_4.dms_dest))

df_tons_predicted_4 = df_tons_predicted_4.withColumn('dms_orig',user_func(df_tons_predicted_4.dms_orig))
df_tons_predicted_4 = df_tons_predicted_4.withColumn('dms_dest',user_func(df_tons_predicted_4.dms_dest))

df_tons_train_5 = df_tons_train_5.withColumn('dms_orig',user_func(df_tons_train_5.dms_orig))
df_tons_train_5 = df_tons_train_5.withColumn('dms_dest',user_func(df_tons_train_5.dms_dest))

df_tons_predicted_5 = df_tons_predicted_5.withColumn('dms_orig',user_func(df_tons_predicted_5.dms_orig))
df_tons_predicted_5 = df_tons_predicted_5.withColumn('dms_dest',user_func(df_tons_predicted_5.dms_dest))

df_tons_train_6 = df_tons_train_6.withColumn('dms_orig',user_func(df_tons_train_6.dms_orig))
df_tons_train_6 = df_tons_train_6.withColumn('dms_dest',user_func(df_tons_train_6.dms_dest))

df_tons_predicted_6 = df_tons_predicted_6.withColumn('dms_orig',user_func(df_tons_predicted_6.dms_orig))
df_tons_predicted_6 = df_tons_predicted_6.withColumn('dms_dest',user_func(df_tons_predicted_6.dms_dest))

df_tons_train_7 = df_tons_train_7.withColumn('dms_orig',user_func(df_tons_train_7.dms_orig))
df_tons_train_7 = df_tons_train_7.withColumn('dms_dest',user_func(df_tons_train_7.dms_dest))

df_tons_predicted_7 = df_tons_predicted_7.withColumn('dms_orig',user_func(df_tons_predicted_7.dms_orig))
df_tons_predicted_7 = df_tons_predicted_7.withColumn('dms_dest',user_func(df_tons_predicted_7.dms_dest))

df_tons_train_8 = df_tons_train_8.withColumn('dms_orig',user_func(df_tons_train_8.dms_orig))
df_tons_train_8 = df_tons_train_8.withColumn('dms_dest',user_func(df_tons_train_8.dms_dest))

df_tons_predicted_8 = df_tons_predicted_8.withColumn('dms_orig',user_func(df_tons_predicted_8.dms_orig))
df_tons_predicted_8 = df_tons_predicted_8.withColumn('dms_dest',user_func(df_tons_predicted_8.dms_dest))

df_tons_train_9 = df_tons_train_9.withColumn('dms_orig',user_func(df_tons_train_9.dms_orig))
df_tons_train_9 = df_tons_train_9.withColumn('dms_dest',user_func(df_tons_train_9.dms_dest))

df_tons_predicted_9 = df_tons_predicted_9.withColumn('dms_orig',user_func(df_tons_predicted_9.dms_orig))
df_tons_predicted_9 = df_tons_predicted_9.withColumn('dms_dest',user_func(df_tons_predicted_9.dms_dest))

df_tons_train_10 = df_tons_train_10.withColumn('dms_orig',user_func(df_tons_train_10.dms_orig))
df_tons_train_10 = df_tons_train_10.withColumn('dms_dest',user_func(df_tons_train_10.dms_dest))

df_tons_predicted_10 = df_tons_predicted_10.withColumn('dms_orig',user_func(df_tons_predicted_10.dms_orig))
df_tons_predicted_10 = df_tons_predicted_10.withColumn('dms_dest',user_func(df_tons_predicted_10.dms_dest))

df_tons_train_11 = df_tons_train_11.withColumn('dms_orig',user_func(df_tons_train_11.dms_orig))
df_tons_train_11 = df_tons_train_11.withColumn('dms_dest',user_func(df_tons_train_11.dms_dest))

df_tons_predicted_11 = df_tons_predicted_11.withColumn('dms_orig',user_func(df_tons_predicted_11.dms_orig))
df_tons_predicted_11 = df_tons_predicted_11.withColumn('dms_dest',user_func(df_tons_predicted_11.dms_dest))

df_tons_train_12 = df_tons_train_12.withColumn('dms_orig',user_func(df_tons_train_12.dms_orig))
df_tons_train_12 = df_tons_train_12.withColumn('dms_dest',user_func(df_tons_train_12.dms_dest))

df_tons_predicted_12 = df_tons_predicted_12.withColumn('dms_orig',user_func(df_tons_predicted_12.dms_orig))
df_tons_predicted_12 = df_tons_predicted_12.withColumn('dms_dest',user_func(df_tons_predicted_12.dms_dest))

df_tons_train_13 = df_tons_train_13.withColumn('dms_orig',user_func(df_tons_train_13.dms_orig))
df_tons_train_13 = df_tons_train_13.withColumn('dms_dest',user_func(df_tons_train_13.dms_dest))

df_tons_predicted_13 = df_tons_predicted_13.withColumn('dms_orig',user_func(df_tons_predicted_13.dms_orig))
df_tons_predicted_13 = df_tons_predicted_13.withColumn('dms_dest',user_func(df_tons_predicted_13.dms_dest))

df_tons_train_14 = df_tons_train_14.withColumn('dms_orig',user_func(df_tons_train_14.dms_orig))
df_tons_train_14 = df_tons_train_14.withColumn('dms_dest',user_func(df_tons_train_14.dms_dest))

df_tons_predicted_14 = df_tons_predicted_14.withColumn('dms_orig',user_func(df_tons_predicted_14.dms_orig))
df_tons_predicted_14 = df_tons_predicted_14.withColumn('dms_dest',user_func(df_tons_predicted_14.dms_dest))

df_tons_train_15 = df_tons_train_15.withColumn('dms_orig',user_func(df_tons_train_15.dms_orig))
df_tons_train_15 = df_tons_train_15.withColumn('dms_dest',user_func(df_tons_train_15.dms_dest))

df_tons_predicted_15 = df_tons_predicted_15.withColumn('dms_orig',user_func(df_tons_predicted_15.dms_orig))
df_tons_predicted_15 = df_tons_predicted_15.withColumn('dms_dest',user_func(df_tons_predicted_15.dms_dest))

df_tons_train_16 = df_tons_train_16.withColumn('dms_orig',user_func(df_tons_train_16.dms_orig))
df_tons_train_16 = df_tons_train_16.withColumn('dms_dest',user_func(df_tons_train_16.dms_dest))

df_tons_predicted_16 = df_tons_predicted_16.withColumn('dms_orig',user_func(df_tons_predicted_16.dms_orig))
df_tons_predicted_16 = df_tons_predicted_16.withColumn('dms_dest',user_func(df_tons_predicted_16.dms_dest))

df_tons_train_17 = df_tons_train_17.withColumn('dms_orig',user_func(df_tons_train_17.dms_orig))
df_tons_train_17 = df_tons_train_17.withColumn('dms_dest',user_func(df_tons_train_17.dms_dest))

df_tons_predicted_17 = df_tons_predicted_17.withColumn('dms_orig',user_func(df_tons_predicted_17.dms_orig))
df_tons_predicted_17 = df_tons_predicted_17.withColumn('dms_dest',user_func(df_tons_predicted_17.dms_dest))

df_tons_train_18 = df_tons_train_18.withColumn('dms_orig',user_func(df_tons_train_18.dms_orig))
df_tons_train_18 = df_tons_train_18.withColumn('dms_dest',user_func(df_tons_train_18.dms_dest))

df_tons_predicted_18 = df_tons_predicted_18.withColumn('dms_orig',user_func(df_tons_predicted_18.dms_orig))
df_tons_predicted_18 = df_tons_predicted_18.withColumn('dms_dest',user_func(df_tons_predicted_18.dms_dest))

df_tons_train_19 = df_tons_train_19.withColumn('dms_orig',user_func(df_tons_train_19.dms_orig))
df_tons_train_19 = df_tons_train_19.withColumn('dms_dest',user_func(df_tons_train_19.dms_dest))

df_tons_predicted_19 = df_tons_predicted_19.withColumn('dms_orig',user_func(df_tons_predicted_19.dms_orig))
df_tons_predicted_19 = df_tons_predicted_19.withColumn('dms_dest',user_func(df_tons_predicted_19.dms_dest))

df_tons_train_20 = df_tons_train_20.withColumn('dms_orig',user_func(df_tons_train_20.dms_orig))
df_tons_train_20 = df_tons_train_20.withColumn('dms_dest',user_func(df_tons_train_20.dms_dest))

df_tons_predicted_20 = df_tons_predicted_20.withColumn('dms_orig',user_func(df_tons_predicted_20.dms_orig))
df_tons_predicted_20 = df_tons_predicted_20.withColumn('dms_dest',user_func(df_tons_predicted_20.dms_dest))

df_tons_train_21 = df_tons_train_21.withColumn('dms_orig',user_func(df_tons_train_21.dms_orig))
df_tons_train_21 = df_tons_train_21.withColumn('dms_dest',user_func(df_tons_train_21.dms_dest))

df_tons_predicted_21 = df_tons_predicted_21.withColumn('dms_orig',user_func(df_tons_predicted_21.dms_orig))
df_tons_predicted_21 = df_tons_predicted_21.withColumn('dms_dest',user_func(df_tons_predicted_21.dms_dest))

df_tons_train_22 = df_tons_train_22.withColumn('dms_orig',user_func(df_tons_train_22.dms_orig))
df_tons_train_22 = df_tons_train_22.withColumn('dms_dest',user_func(df_tons_train_22.dms_dest))

df_tons_predicted_22 = df_tons_predicted_22.withColumn('dms_orig',user_func(df_tons_predicted_22.dms_orig))
df_tons_predicted_22 = df_tons_predicted_22.withColumn('dms_dest',user_func(df_tons_predicted_22.dms_dest))

Commodity_dic={1 : "Animals and Fish (live)" , 2 : "Cereal Grains (includes seed)" , 3 : "Agricultural Products (excludes Animal Feed, Cereal Grains,Forage Products)" , 4 : "Animal Feed, Eggs, Honey, Other Products of Animal Origin" , 5 : "Meat, Poultry, Fish, Seafood, and Their Preparations" , 6 : "Milled Grain Products and Preparations, and Bakery Products" , 7 : "Other Prepared Foodstuffs, Fats and Oils" , 8 : "Alcoholic Beverages and Denatured Alcohol" , 9 : "Tobacco Products" , 10 : "Monumental or Building Stone" , 11 : "Natural Sands" , 12 : "Gravel and Crushed Stone (excludes Dolomite and Slate)" , 13 : "Other Non-Metallic Minerals not elsewhere classified" , 14 : "Metallic Ores and Concentrates" , 15 : "Coal" , 16 : "Crude Petroleum" , 17 : "Gasoline, Aviation Turbine Fuel, and Ethanol (includes Kerosene, and Fuel Alcohols)" , 18 : "Fuel Oils (includes Diesel, Bunker C, and Biodiesel)" , 19 : "Other Coal and Petroleum Products, not elsewhere classified" , 20 : "Basic Chemicals" , 21 : "Pharmaceutical Products" , 22 : "Fertilizers" , 23 : "Other Chemical Products and Preparations" , 24 : "Plastics and Rubber" , 25 : "Logs and Other Wood in the Rough" , 26 : "Wood Products" ,  27 : "Pulp, Newsprint, Paper, and Paperboard" , 28 : "Paper or Paperboard Articles" , 29 : "Printed Products" , 30 : "Textiles, Leather, and Articles of Textiles or Leather" , 31 : "Non-Metallic Mineral Products" , 32 : "Base Metal in Primary or Semi-Finished Forms and in Finished Basic Shapes" , 33 : "Articles of Base Metal" , 34 : "Machinery" , 35 : "Electronic and Other Electrical Equipment and Components, and Office Equipment" , 36 : "Motorized and Other Vehicles (includes parts)" , 37 : "Transportation Equipment, not elsewhere classified" ,  38 : "Precision Instruments and Apparatus" , 39 : "Furniture, Mattresses and Mattress Supports, Lamps, Lighting Fittings, and Illuminated Signs" , 40 : "Miscellaneous Manufactured Products" , 41 : "Waste and Scrap (excludes of agriculture or food)" , 43 : "Mixed Freight" , 99 : "Commodity unknown"}

com_user_func =  udf(lambda x: Commodity_dic.get(x), StringType())

df_12_18 = df_12_18.withColumn('sctg2',com_user_func(df_12_18.sctg2))


df_tons_train_1 = df_tons_train_1.withColumn('sctg2',com_user_func(df_tons_train_1.sctg2))
df_tons_predicted_1 = df_tons_predicted_1.withColumn('sctg2',com_user_func(df_tons_predicted_1.sctg2))

df_tons_train_2 = df_tons_train_2.withColumn('sctg2',com_user_func(df_tons_train_2.sctg2))
df_tons_predicted_2 = df_tons_predicted_2.withColumn('sctg2',com_user_func(df_tons_predicted_2.sctg2))

df_tons_train_3 = df_tons_train_3.withColumn('sctg2',com_user_func(df_tons_train_3.sctg2))
df_tons_predicted_3 = df_tons_predicted_3.withColumn('sctg2',com_user_func(df_tons_predicted_3.sctg2))

df_tons_train_4 = df_tons_train_4.withColumn('sctg2',com_user_func(df_tons_train_4.sctg2))
df_tons_predicted_4 = df_tons_predicted_4.withColumn('sctg2',com_user_func(df_tons_predicted_4.sctg2))

df_tons_train_5 = df_tons_train_5.withColumn('sctg2',com_user_func(df_tons_train_5.sctg2))
df_tons_predicted_5 = df_tons_predicted_5.withColumn('sctg2',com_user_func(df_tons_predicted_5.sctg2))

df_tons_train_6 = df_tons_train_6.withColumn('sctg2',com_user_func(df_tons_train_6.sctg2))
df_tons_predicted_6 = df_tons_predicted_6.withColumn('sctg2',com_user_func(df_tons_predicted_6.sctg2))

df_tons_train_7 = df_tons_train_7.withColumn('sctg2',com_user_func(df_tons_train_7.sctg2))
df_tons_predicted_7 = df_tons_predicted_7.withColumn('sctg2',com_user_func(df_tons_predicted_7.sctg2))

df_tons_train_8 = df_tons_train_8.withColumn('sctg2',com_user_func(df_tons_train_8.sctg2))
df_tons_predicted_8 = df_tons_predicted_8.withColumn('sctg2',com_user_func(df_tons_predicted_8.sctg2))

df_tons_train_9 = df_tons_train_9.withColumn('sctg2',com_user_func(df_tons_train_9.sctg2))
df_tons_predicted_9 = df_tons_predicted_9.withColumn('sctg2',com_user_func(df_tons_predicted_9.sctg2))

df_tons_train_10 = df_tons_train_10.withColumn('sctg2',com_user_func(df_tons_train_10.sctg2))
df_tons_predicted_10 = df_tons_predicted_10.withColumn('sctg2',com_user_func(df_tons_predicted_10.sctg2))

df_tons_train_11 = df_tons_train_11.withColumn('sctg2',com_user_func(df_tons_train_11.sctg2))
df_tons_predicted_11 = df_tons_predicted_11.withColumn('sctg2',com_user_func(df_tons_predicted_11.sctg2))

df_tons_train_12 = df_tons_train_12.withColumn('sctg2',com_user_func(df_tons_train_12.sctg2))
df_tons_predicted_12 = df_tons_predicted_12.withColumn('sctg2',com_user_func(df_tons_predicted_12.sctg2))

df_tons_train_13 = df_tons_train_13.withColumn('sctg2',com_user_func(df_tons_train_13.sctg2))
df_tons_predicted_13 = df_tons_predicted_13.withColumn('sctg2',com_user_func(df_tons_predicted_13.sctg2))

df_tons_train_14 = df_tons_train_14.withColumn('sctg2',com_user_func(df_tons_train_14.sctg2))
df_tons_predicted_14 = df_tons_predicted_14.withColumn('sctg2',com_user_func(df_tons_predicted_14.sctg2))

df_tons_train_15 = df_tons_train_15.withColumn('sctg2',com_user_func(df_tons_train_15.sctg2))
df_tons_predicted_15 = df_tons_predicted_15.withColumn('sctg2',com_user_func(df_tons_predicted_15.sctg2))

df_tons_train_16 = df_tons_train_16.withColumn('sctg2',com_user_func(df_tons_train_16.sctg2))
df_tons_predicted_16 = df_tons_predicted_16.withColumn('sctg2',com_user_func(df_tons_predicted_16.sctg2))

df_tons_train_17 = df_tons_train_17.withColumn('sctg2',com_user_func(df_tons_train_17.sctg2))
df_tons_predicted_17 = df_tons_predicted_17.withColumn('sctg2',com_user_func(df_tons_predicted_17.sctg2))

df_tons_train_18 = df_tons_train_18.withColumn('sctg2',com_user_func(df_tons_train_18.sctg2))
df_tons_predicted_18 = df_tons_predicted_18.withColumn('sctg2',com_user_func(df_tons_predicted_18.sctg2))

df_tons_train_19 = df_tons_train_19.withColumn('sctg2',com_user_func(df_tons_train_19.sctg2))
df_tons_predicted_19 = df_tons_predicted_19.withColumn('sctg2',com_user_func(df_tons_predicted_19.sctg2))

df_tons_train_20 = df_tons_train_20.withColumn('sctg2',com_user_func(df_tons_train_20.sctg2))
df_tons_predicted_20 = df_tons_predicted_20.withColumn('sctg2',com_user_func(df_tons_predicted_20.sctg2))

df_tons_train_21 = df_tons_train_21.withColumn('sctg2',com_user_func(df_tons_train_21.sctg2))
df_tons_predicted_21 = df_tons_predicted_21.withColumn('sctg2',com_user_func(df_tons_predicted_21.sctg2))

df_tons_train_22 = df_tons_train_22.withColumn('sctg2',com_user_func(df_tons_train_22.sctg2))
df_tons_predicted_22 = df_tons_predicted_22.withColumn('sctg2',com_user_func(df_tons_predicted_22.sctg2))


Mode_dic={1 : "Truck" , 2 : "Rail" , 3 : "Water" , 4 : "Air (includes truck-air)" , 5 : "Multiple Modes and Mail" , 6 : "Pipeline" , 7 : "Other and Unknown" , 8 : "No Domestic Mode"}

Mode_user_func =  udf(lambda x: Mode_dic.get(x), StringType())

df_12_18 = df_12_18.withColumn('dms_mode',Mode_user_func(df_12_18.dms_mode))

df_tons_train_1 = df_tons_train_1.withColumn('dms_mode',Mode_user_func(df_tons_train_1.dms_mode))
df_tons_predicted_1 = df_tons_predicted_1.withColumn('dms_mode',Mode_user_func(df_tons_predicted_1.dms_mode))

df_tons_train_2 = df_tons_train_2.withColumn('dms_mode',Mode_user_func(df_tons_train_2.dms_mode))
df_tons_predicted_2 = df_tons_predicted_2.withColumn('dms_mode',Mode_user_func(df_tons_predicted_2.dms_mode))

df_tons_train_3 = df_tons_train_3.withColumn('dms_mode',Mode_user_func(df_tons_train_3.dms_mode))
df_tons_predicted_3 = df_tons_predicted_3.withColumn('dms_mode',Mode_user_func(df_tons_predicted_3.dms_mode))

df_tons_train_4 = df_tons_train_4.withColumn('dms_mode',Mode_user_func(df_tons_train_4.dms_mode))
df_tons_predicted_4 = df_tons_predicted_4.withColumn('dms_mode',Mode_user_func(df_tons_predicted_4.dms_mode))

df_tons_train_5 = df_tons_train_5.withColumn('dms_mode',Mode_user_func(df_tons_train_5.dms_mode))
df_tons_predicted_5 = df_tons_predicted_5.withColumn('dms_mode',Mode_user_func(df_tons_predicted_5.dms_mode))

df_tons_train_6 = df_tons_train_6.withColumn('dms_mode',Mode_user_func(df_tons_train_6.dms_mode))
df_tons_predicted_6 = df_tons_predicted_6.withColumn('dms_mode',Mode_user_func(df_tons_predicted_6.dms_mode))

df_tons_train_7 = df_tons_train_7.withColumn('dms_mode',Mode_user_func(df_tons_train_7.dms_mode))
df_tons_predicted_7 = df_tons_predicted_7.withColumn('dms_mode',Mode_user_func(df_tons_predicted_7.dms_mode))

df_tons_train_8 = df_tons_train_8.withColumn('dms_mode',Mode_user_func(df_tons_train_8.dms_mode))
df_tons_predicted_8 = df_tons_predicted_8.withColumn('dms_mode',Mode_user_func(df_tons_predicted_8.dms_mode))

df_tons_train_9 = df_tons_train_9.withColumn('dms_mode',Mode_user_func(df_tons_train_9.dms_mode))
df_tons_predicted_9 = df_tons_predicted_9.withColumn('dms_mode',Mode_user_func(df_tons_predicted_9.dms_mode))

df_tons_train_10 = df_tons_train_10.withColumn('dms_mode',Mode_user_func(df_tons_train_10.dms_mode))
df_tons_predicted_10 = df_tons_predicted_10.withColumn('dms_mode',Mode_user_func(df_tons_predicted_10.dms_mode))

df_tons_train_11 = df_tons_train_11.withColumn('dms_mode',Mode_user_func(df_tons_train_11.dms_mode))
df_tons_predicted_11 = df_tons_predicted_11.withColumn('dms_mode',Mode_user_func(df_tons_predicted_11.dms_mode))

df_tons_train_12 = df_tons_train_12.withColumn('dms_mode',Mode_user_func(df_tons_train_12.dms_mode))
df_tons_predicted_12 = df_tons_predicted_12.withColumn('dms_mode',Mode_user_func(df_tons_predicted_12.dms_mode))

df_tons_train_13 = df_tons_train_13.withColumn('dms_mode',Mode_user_func(df_tons_train_13.dms_mode))
df_tons_predicted_13 = df_tons_predicted_13.withColumn('dms_mode',Mode_user_func(df_tons_predicted_13.dms_mode))

df_tons_train_14 = df_tons_train_14.withColumn('dms_mode',Mode_user_func(df_tons_train_14.dms_mode))
df_tons_predicted_14 = df_tons_predicted_14.withColumn('dms_mode',Mode_user_func(df_tons_predicted_14.dms_mode))

df_tons_train_15 = df_tons_train_15.withColumn('dms_mode',Mode_user_func(df_tons_train_15.dms_mode))
df_tons_predicted_15 = df_tons_predicted_15.withColumn('dms_mode',Mode_user_func(df_tons_predicted_15.dms_mode))

df_tons_train_16 = df_tons_train_16.withColumn('dms_mode',Mode_user_func(df_tons_train_16.dms_mode))
df_tons_predicted_16 = df_tons_predicted_16.withColumn('dms_mode',Mode_user_func(df_tons_predicted_16.dms_mode))

df_tons_train_17 = df_tons_train_17.withColumn('dms_mode',Mode_user_func(df_tons_train_17.dms_mode))
df_tons_predicted_17 = df_tons_predicted_17.withColumn('dms_mode',Mode_user_func(df_tons_predicted_17.dms_mode))

df_tons_train_18 = df_tons_train_18.withColumn('dms_mode',Mode_user_func(df_tons_train_18.dms_mode))
df_tons_predicted_18 = df_tons_predicted_18.withColumn('dms_mode',Mode_user_func(df_tons_predicted_18.dms_mode))

df_tons_train_19 = df_tons_train_19.withColumn('dms_mode',Mode_user_func(df_tons_train_19.dms_mode))
df_tons_predicted_19 = df_tons_predicted_19.withColumn('dms_mode',Mode_user_func(df_tons_predicted_19.dms_mode))

df_tons_train_20 = df_tons_train_20.withColumn('dms_mode',Mode_user_func(df_tons_train_20.dms_mode))
df_tons_predicted_20 = df_tons_predicted_20.withColumn('dms_mode',Mode_user_func(df_tons_predicted_20.dms_mode))

df_tons_train_21 = df_tons_train_21.withColumn('dms_mode',Mode_user_func(df_tons_train_21.dms_mode))
df_tons_predicted_21 = df_tons_predicted_21.withColumn('dms_mode',Mode_user_func(df_tons_predicted_21.dms_mode))

df_tons_train_22 = df_tons_train_22.withColumn('dms_mode',Mode_user_func(df_tons_train_22.dms_mode))
df_tons_predicted_22 = df_tons_predicted_22.withColumn('dms_mode',Mode_user_func(df_tons_predicted_22.dms_mode))

encoding_var = [i[0] for i in df_tons_train_1.dtypes if (i[1]=='string') & (i[0]!='tons') & (i[0]!='dms_orig')]
#print("encoding_var:",encoding_var)

num_var = [i[0] for i in df_tons_train_1.dtypes if ((i[1]=='int') | (i[1]=='double') | (i[1]=='vector')) & (i[0]!='tons') & (i[0]!='year') & (i[0]!='wgt_dist')]
#print("num_var:",num_var)

assemblers_scaled = [VectorAssembler(inputCols=[col], outputCol=col + "_vecc") for col in num_var]
scalers = [MinMaxScaler(inputCol=col + "_vecc", outputCol=col + "_scaled") for col in num_var]
string_indexes = [StringIndexer(inputCol = c, outputCol = 'IDX_' + c, handleInvalid = 'keep') for c in encoding_var]
onehot_indexes = [OneHotEncoderEstimator(inputCols = ['IDX_' + c], outputCols = ['OHE_' + c]) for c in encoding_var]
assembler = VectorAssembler(inputCols = [col + "_scaled" for col in num_var] + ['OHE_' + c for c in encoding_var], outputCol = "features")
#assembler = VectorAssembler(inputCols = num_var + ['OHE_' + c for c in encoding_var], outputCol = "features")

from pyspark.ml.regression import RandomForestRegressor
RF = RandomForestRegressor(featuresCol = "features" ,labelCol="tons", numTrees=20,maxDepth=30,maxBins=17)
#from pyspark.ml.regression import GBTRegressor
#GBT = GBTRegressor(featuresCol = "features",maxIter=5 ,labelCol="tons",maxDepth=30,maxBins=17)
#GBT = GBTRegressor(featuresCol = "features" ,labelCol="tons")
#from pyspark.ml.regression import DecisionTreeRegressor
#Dt = DecisionTreeRegressor(featuresCol = "features", labelCol="tons",maxDepth=12,maxBins=8)
pipeline = Pipeline(stages=assemblers_scaled + scalers + string_indexes + onehot_indexes + [assembler,RF])
#pipeline = Pipeline(stages= string_indexes + onehot_indexes + [assembler,Dt])

model_1=pipeline.fit(df_tons_train_1)
df_tons_predicted_1= model_1.transform(df_tons_predicted_1)
model_2=pipeline.fit(df_tons_train_2)
df_tons_predicted_2= model_2.transform(df_tons_predicted_2)
model_3=pipeline.fit(df_tons_train_3)
df_tons_predicted_3= model_3.transform(df_tons_predicted_3)
model_4=pipeline.fit(df_tons_train_4)
df_tons_predicted_4= model_4.transform(df_tons_predicted_4)
model_5=pipeline.fit(df_tons_train_5)
df_tons_predicted_5= model_5.transform(df_tons_predicted_5)
model_6=pipeline.fit(df_tons_train_6)
df_tons_predicted_6= model_6.transform(df_tons_predicted_6)
model_7=pipeline.fit(df_tons_train_7)
df_tons_predicted_7= model_7.transform(df_tons_predicted_7)
model_8=pipeline.fit(df_tons_train_8)
df_tons_predicted_8= model_8.transform(df_tons_predicted_8)
model_9=pipeline.fit(df_tons_train_9)
df_tons_predicted_9= model_9.transform(df_tons_predicted_9)
model_10=pipeline.fit(df_tons_train_10)
df_tons_predicted_10= model_10.transform(df_tons_predicted_10)
model_11=pipeline.fit(df_tons_train_11)
df_tons_predicted_11= model_11.transform(df_tons_predicted_11)
model_12=pipeline.fit(df_tons_train_12)
df_tons_predicted_12= model_12.transform(df_tons_predicted_12)
model_13=pipeline.fit(df_tons_train_13)
df_tons_predicted_13= model_13.transform(df_tons_predicted_13)
model_14=pipeline.fit(df_tons_train_14)
df_tons_predicted_14= model_14.transform(df_tons_predicted_14)
model_15=pipeline.fit(df_tons_train_15)
df_tons_predicted_15= model_15.transform(df_tons_predicted_15)
model_16=pipeline.fit(df_tons_train_16)
df_tons_predicted_16= model_16.transform(df_tons_predicted_16)
model_17=pipeline.fit(df_tons_train_17)
df_tons_predicted_17= model_17.transform(df_tons_predicted_17)
model_18=pipeline.fit(df_tons_train_18)
df_tons_predicted_18= model_18.transform(df_tons_predicted_18)
model_19=pipeline.fit(df_tons_train_19)
df_tons_predicted_19= model_19.transform(df_tons_predicted_19)
model_20=pipeline.fit(df_tons_train_20)
df_tons_predicted_20= model_20.transform(df_tons_predicted_20)
model_21=pipeline.fit(df_tons_train_21)
df_tons_predicted_21= model_21.transform(df_tons_predicted_21)
model_22=pipeline.fit(df_tons_train_22)
df_tons_predicted_22= model_22.transform(df_tons_predicted_22)

df_tons_predicted_1=df_tons_predicted_1.select("year","dms_orig","dms_dest","dms_mode","sctg2","wgt_dist","prediction")
df_tons_predicted_2=df_tons_predicted_2.select("year","dms_orig","dms_dest","dms_mode","sctg2","wgt_dist","prediction")
df_tons_predicted_3=df_tons_predicted_3.select("year","dms_orig","dms_dest","dms_mode","sctg2","wgt_dist","prediction")
df_tons_predicted_4=df_tons_predicted_4.select("year","dms_orig","dms_dest","dms_mode","sctg2","wgt_dist","prediction")
df_tons_predicted_5=df_tons_predicted_5.select("year","dms_orig","dms_dest","dms_mode","sctg2","wgt_dist","prediction")
df_tons_predicted_6=df_tons_predicted_6.select("year","dms_orig","dms_dest","dms_mode","sctg2","wgt_dist","prediction")
df_tons_predicted_7=df_tons_predicted_7.select("year","dms_orig","dms_dest","dms_mode","sctg2","wgt_dist","prediction")
df_tons_predicted_8=df_tons_predicted_8.select("year","dms_orig","dms_dest","dms_mode","sctg2","wgt_dist","prediction")
df_tons_predicted_9=df_tons_predicted_9.select("year","dms_orig","dms_dest","dms_mode","sctg2","wgt_dist","prediction")
df_tons_predicted_10=df_tons_predicted_10.select("year","dms_orig","dms_dest","dms_mode","sctg2","wgt_dist","prediction")
df_tons_predicted_11=df_tons_predicted_11.select("year","dms_orig","dms_dest","dms_mode","sctg2","wgt_dist","prediction")
df_tons_predicted_12=df_tons_predicted_12.select("year","dms_orig","dms_dest","dms_mode","sctg2","wgt_dist","prediction")
df_tons_predicted_13=df_tons_predicted_13.select("year","dms_orig","dms_dest","dms_mode","sctg2","wgt_dist","prediction")
df_tons_predicted_14=df_tons_predicted_14.select("year","dms_orig","dms_dest","dms_mode","sctg2","wgt_dist","prediction")
df_tons_predicted_15=df_tons_predicted_15.select("year","dms_orig","dms_dest","dms_mode","sctg2","wgt_dist","prediction")
df_tons_predicted_16=df_tons_predicted_16.select("year","dms_orig","dms_dest","dms_mode","sctg2","wgt_dist","prediction")
df_tons_predicted_17=df_tons_predicted_17.select("year","dms_orig","dms_dest","dms_mode","sctg2","wgt_dist","prediction")
df_tons_predicted_18=df_tons_predicted_18.select("year","dms_orig","dms_dest","dms_mode","sctg2","wgt_dist","prediction")
df_tons_predicted_19=df_tons_predicted_19.select("year","dms_orig","dms_dest","dms_mode","sctg2","wgt_dist","prediction")
df_tons_predicted_20=df_tons_predicted_20.select("year","dms_orig","dms_dest","dms_mode","sctg2","wgt_dist","prediction")
df_tons_predicted_21=df_tons_predicted_21.select("year","dms_orig","dms_dest","dms_mode","sctg2","wgt_dist","prediction")
df_tons_predicted_22=df_tons_predicted_22.select("year","dms_orig","dms_dest","dms_mode","sctg2","wgt_dist","prediction")


df_tons_forecasted_1_24=df_tons_predicted_1.filter(col('year').isin([2024]))
df_tons_forecasted_2_24=df_tons_predicted_2.filter(col('year').isin([2024]))
df_tons_forecasted_3_24=df_tons_predicted_3.filter(col('year').isin([2024]))
df_tons_forecasted_4_24=df_tons_predicted_4.filter(col('year').isin([2024]))
df_tons_forecasted_5_24=df_tons_predicted_5.filter(col('year').isin([2024]))
df_tons_forecasted_6_24=df_tons_predicted_6.filter(col('year').isin([2024]))
df_tons_forecasted_7_24=df_tons_predicted_7.filter(col('year').isin([2024]))
df_tons_forecasted_8_24=df_tons_predicted_8.filter(col('year').isin([2024]))
df_tons_forecasted_9_24=df_tons_predicted_9.filter(col('year').isin([2024]))
df_tons_forecasted_10_24=df_tons_predicted_10.filter(col('year').isin([2024]))
df_tons_forecasted_11_24=df_tons_predicted_11.filter(col('year').isin([2024]))
df_tons_forecasted_12_24=df_tons_predicted_12.filter(col('year').isin([2024]))
df_tons_forecasted_13_24=df_tons_predicted_13.filter(col('year').isin([2024]))
df_tons_forecasted_14_24=df_tons_predicted_14.filter(col('year').isin([2024]))
df_tons_forecasted_15_24=df_tons_predicted_15.filter(col('year').isin([2024]))
df_tons_forecasted_16_24=df_tons_predicted_16.filter(col('year').isin([2024]))
df_tons_forecasted_17_24=df_tons_predicted_17.filter(col('year').isin([2024]))
df_tons_forecasted_18_24=df_tons_predicted_18.filter(col('year').isin([2024]))
df_tons_forecasted_19_24=df_tons_predicted_19.filter(col('year').isin([2024]))
df_tons_forecasted_20_24=df_tons_predicted_20.filter(col('year').isin([2024]))
df_tons_forecasted_21_24=df_tons_predicted_21.filter(col('year').isin([2024]))
df_tons_forecasted_22_24=df_tons_predicted_22.filter(col('year').isin([2024]))



df_tons_forecasted_1_24.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_forecasted_1_24.csv")
df_tons_forecasted_2_24.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_forecasted_2_24.csv")
df_tons_forecasted_3_24.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_forecasted_3_24.csv")
df_tons_forecasted_4_24.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_forecasted_4_24.csv")
df_tons_forecasted_5_24.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_forecasted_5_24.csv")
df_tons_forecasted_6_24.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_forecasted_6_24.csv")
df_tons_forecasted_7_24.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_forecasted_7_24.csv")
df_tons_forecasted_8_24.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_forecasted_8_24.csv")
df_tons_forecasted_9_24.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_forecasted_9_24.csv")
df_tons_forecasted_10_24.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_forecasted_10_24.csv")
df_tons_forecasted_11_24.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_forecasted_11_24.csv")
df_tons_forecasted_12_24.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_forecasted_12_24.csv")
df_tons_forecasted_13_24.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_forecasted_13_24.csv")
df_tons_forecasted_14_24.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_forecasted_14_24.csv")
df_tons_forecasted_15_24.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_forecasted_15_24.csv")
df_tons_forecasted_16_24.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_forecasted_16_24.csv")
df_tons_forecasted_17_24.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_forecasted_17_24.csv")
df_tons_forecasted_18_24.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_forecasted_18_24.csv")
df_tons_forecasted_19_24.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_forecasted_19_24.csv")
df_tons_forecasted_20_24.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_forecasted_20_24.csv")
df_tons_forecasted_21_24.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_forecasted_21_24.csv")
df_tons_forecasted_22_24.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_tons_forecasted_22_24.csv")



df_tons_forecasted_1_24 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_forecasted_1_24.csv",header=True, inferSchema=True)
df_tons_forecasted_2_24 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_forecasted_2_24.csv",header=True, inferSchema=True)
df_tons_forecasted_3_24 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_forecasted_3_24.csv",header=True, inferSchema=True)
df_tons_forecasted_4_24 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_forecasted_4_24.csv",header=True, inferSchema=True)
df_tons_forecasted_5_24 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_forecasted_5_24.csv",header=True, inferSchema=True)
df_tons_forecasted_6_24 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_forecasted_6_24.csv",header=True, inferSchema=True)
df_tons_forecasted_7_24 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_forecasted_7_24.csv",header=True, inferSchema=True)
df_tons_forecasted_8_24 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_forecasted_8_24.csv",header=True, inferSchema=True)
df_tons_forecasted_9_24 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_forecasted_9_24.csv",header=True, inferSchema=True)
df_tons_forecasted_10_24 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_forecasted_10_24.csv",header=True, inferSchema=True)
df_tons_forecasted_11_24 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_forecasted_11_24.csv",header=True, inferSchema=True)
df_tons_forecasted_12_24 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_forecasted_12_24.csv",header=True, inferSchema=True)
df_tons_forecasted_13_24 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_forecasted_13_24.csv",header=True, inferSchema=True)
df_tons_forecasted_14_24 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_forecasted_14_24.csv",header=True, inferSchema=True)
df_tons_forecasted_15_24 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_forecasted_15_24.csv",header=True, inferSchema=True)
df_tons_forecasted_16_24 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_forecasted_16_24.csv",header=True, inferSchema=True)
df_tons_forecasted_17_24 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_forecasted_17_24.csv",header=True, inferSchema=True)
df_tons_forecasted_18_24 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_forecasted_18_24.csv",header=True, inferSchema=True)
df_tons_forecasted_19_24 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_forecasted_19_24.csv",header=True, inferSchema=True)
df_tons_forecasted_20_24 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_forecasted_20_24.csv",header=True, inferSchema=True)
df_tons_forecasted_21_24 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_forecasted_21_24.csv",header=True, inferSchema=True)
df_tons_forecasted_22_24 = spark.read.csv("file:///home/abdullah/Downloads/df_tons_forecasted_22_24.csv",header=True, inferSchema=True)

df_tons_forecasted_24 = df_tons_forecasted_1_24.union(df_tons_forecasted_2_24)
df_tons_forecasted_24 = df_tons_forecasted_24.union(df_tons_forecasted_3_24)
df_tons_forecasted_24 = df_tons_forecasted_24.union(df_tons_forecasted_4_24)
df_tons_forecasted_24 = df_tons_forecasted_24.union(df_tons_forecasted_5_24)
df_tons_forecasted_24 = df_tons_forecasted_24.union(df_tons_forecasted_6_24)
df_tons_forecasted_24 = df_tons_forecasted_24.union(df_tons_forecasted_7_24)
df_tons_forecasted_24 = df_tons_forecasted_24.union(df_tons_forecasted_8_24)
df_tons_forecasted_24 = df_tons_forecasted_24.union(df_tons_forecasted_9_24)
df_tons_forecasted_24 = df_tons_forecasted_24.union(df_tons_forecasted_10_24)
df_tons_forecasted_24 = df_tons_forecasted_24.union(df_tons_forecasted_11_24)
df_tons_forecasted_24 = df_tons_forecasted_24.union(df_tons_forecasted_12_24)
df_tons_forecasted_24 = df_tons_forecasted_24.union(df_tons_forecasted_13_24)
df_tons_forecasted_24 = df_tons_forecasted_24.union(df_tons_forecasted_14_24)
df_tons_forecasted_24 = df_tons_forecasted_24.union(df_tons_forecasted_15_24)
df_tons_forecasted_24 = df_tons_forecasted_24.union(df_tons_forecasted_16_24)
df_tons_forecasted_24 = df_tons_forecasted_24.union(df_tons_forecasted_17_24)
df_tons_forecasted_24 = df_tons_forecasted_24.union(df_tons_forecasted_18_24)
df_tons_forecasted_24 = df_tons_forecasted_24.union(df_tons_forecasted_19_24)
df_tons_forecasted_24 = df_tons_forecasted_24.union(df_tons_forecasted_20_24)
df_tons_forecasted_24 = df_tons_forecasted_24.union(df_tons_forecasted_21_24)
df_tons_forecasted_24 = df_tons_forecasted_24.union(df_tons_forecasted_22_24)

df_tons_forecasted_24=df_tons_forecasted_24.withColumnRenamed('dms_orig', 'dms_orig_24').withColumnRenamed('dms_dest', 'dms_dest_24').withColumnRenamed('dms_mode', 'dms_mode_24').withColumnRenamed('sctg2', 'sctg2_24').withColumnRenamed('wgt_dist', 'wgt_dist_24').withColumnRenamed('prediction', 'tons_24').drop("year")




df_tons_forecasted_24=df_tons_forecasted_24.withColumnRenamed('dms_orig', 'dms_orig_24').withColumnRenamed('dms_dest', 'dms_dest_24').withColumnRenamed('dms_mode', 'dms_mode_24').withColumnRenamed('sctg2', 'sctg2_24').withColumnRenamed('wgt_dist', 'wgt_dist_24').withColumnRenamed('prediction', 'tons_24').drop("year")



cond_24=[df_12_18.dms_orig == df_tons_forecasted_24.dms_orig_24 , df_12_18.dms_dest == df_tons_forecasted_24.dms_dest_24 , df_12_18.dms_mode == df_tons_forecasted_24.dms_mode_24 , df_12_18.sctg2 == df_tons_forecasted_24.sctg2_24,df_12_18.wgt_dist == df_tons_forecasted_24.wgt_dist_24]

df_Final=df_12_18.join(df_tons_forecasted_24,cond_24).drop("dms_orig_24","dms_dest_24","dms_mode_24","sctg2_24","wgt_dist_24")

#df_Final.count()

#df_Final.show(5)

df_Final.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:///home/abdullah/Downloads/df_Final.csv")


