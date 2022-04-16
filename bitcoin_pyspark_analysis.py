#Import needed modules
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType
import re
import pandas as pd
from pyspark.sql.functions import col

#Create SparkSession with Kyro Serializer
spark_session = SparkSession.builder.config("spark.serializer","org.apache.spark.serializer.KryoSerializer").config("spark.kyro.registrationRequired","false").appName("bitcoin_final").getOrCreate()
spark = SparkSession(sc)

#Read data from the HDFS
df_load=spark.read.csv("hdfs://localhost:9000/bitcoin_final/bitcoin_final.csv",header=True)
df_load.show(5)

#Perform analysis on the data using Queries 
#Quick statistical analysis
statistics_df=df_load.select(col("percent_change_1h").alias("percent_change_one_hour")).describe()
statistics_df.show()

#Analysis of Bitcoin's frequency
bitcoin_freq_df=(df_load.select(col("name").alias("bitcoin_type")).groupBy("bitcoin_type").count().sort("bitcoin_type").cache())
print("Total distinct Bitcoin types:", bitcoin_freq_df.count())
print(bitcoin_freq_df)
bitcoin_freq_df.show(5)

#A look of 1h percent changes of Bitcoin data
paths_df = (df_load.select(col("percent_change_1h").alias("percent-change"),col("name")).groupBy("percent-change").count().sort("count", ascending=False).limit(20))
paths_df.show(20)

#Filter Bitcoin data without the Dent bitcoin type
notbitcoin_df = (df_load.filter(df_load["name"] != "Dent"))
notbitcoin_df.show(5)

#Show types of bitcoin that don't have Dent bitcoin type
freq_bitcoin_df = (notbitcoin_df.select(col("name").alias("bitcoin_type")).groupBy("bitcoin_type").count().sort("count", ascending=False).limit(5))
freq_bitcoin_df.show(truncate=False)

#Import aggregations function modules
from pyspark.sql.functions import col
from pyspark.sql import functions as F

#Print schema
df_load.printSchema()

#Perform avg of 7 day percent change
bitcoin_day_df = df_load.select(F.avg("percent_change_7d").alias("percent"))
bitcoin_day_df.show(5)

#Min and max aggregations
bitcoin_day_df = df_load.select(F.min("price_usd").alias("price_min"))
bitcoin_day_df.show(truncate=False)
bitcoin_day_df = df_load.select(F.max("price_usd").alias("price_max"))
bitcoin_day_df.show(truncate=False)

#Create a final dataframe to save as csv file
final_df = df_load.select(df_load.id.alias("bitcoin_id"), df_load.csupply.alias("bitcoin_supply"), df_load.market_cap_usd.alias("market_cap_in_dollars"), df_load.msupply.alias("msupply"), df_load.name.alias("bitcoin_type"), df_load.nameid.alias("bitcoin_name_id"), df_load.percent_change_1h.alias("percent_change_in_one_hr"), df_load.percent_change_24h.alias("percent_change_in_day"), df_load.percent_change_7d.alias("percent_change_in_one_week"), df_load.price_btc.alias("bitcoin_price"), df_load.price_usd.alias("bitcoin_price_usd"), df_load.rank.alias("bitcoin_rank"), df_load.symbol.alias("bitcoin_symbol"))

final_df.show(5)

#Write the final dataframe results to HDFS and store in CSV format - headers included
final_df.coalesce(1).write.format("csv").option("header", "true").save("hdfs://localhost:9000/bitcoin_pyspark_results3")

