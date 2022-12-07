import findspark
findspark.init()
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.master("local").appName("GAVotesVisual").getOrCreate()

schema = StructType() \
    .add("County Number",IntegerType(),True) \
    .add("Registration Number",IntegerType(),True) \
    .add("Election Date",DateType(),True) \
    .add("Election Type",IntegerType(),True) \
    .add("Party",StringType(),True) \
    .add("Absentee",StringType(),True)\
    .add("Provisional",StringType(),True)\
    .add("Supplemental",StringType(),True)

df = spark.read.options(delimiter=",", header=True,dateFormat="yyyyMMdd", ignoreTrailingWhiteSpace=True).schema(schema).csv("data/votehistory/data/*.csv")

df = df.withColumn("Absentee", F.when(F.col('Absentee') == "Y",True).otherwise(False))
df = df.withColumn('Provisional', F.when(F.col('Provisional') == "Y",True).otherwise(False))
df = df.withColumn('Supplemental', F.when(F.col('Supplemental') == "Y",True).otherwise(False))
df.printSchema()
df.filter(df["Registration Number"] == "07255617").show()

cnt_cond = lambda cond: F.sum(F.when(cond, 1).otherwise(0))

dfVoterHistorySummary = df.groupBy("Registration Number").agg(
    F.count(F.col("Registration Number")).alias("totalElections"),
    cnt_cond(F.col('Election Type').isin([1,2,10])).alias("primaryElections"),
    cnt_cond(F.col('Party') == "D").alias("primaryElectionsD"),
    cnt_cond(F.col('Party') == "R").alias("primaryElectionsR"),
    cnt_cond(F.col('Party') == "NP").alias("primaryElectionsNP"),
    F.sum(F.col("Absentee").cast("long")).alias("Absentee"),
    F.sum(F.col("Provisional").cast("long")).alias("Provisional"),
    F.sum(F.col("Supplemental").cast("long")).alias("Supplemental"),
)
dfVoterHistorySummary = dfVoterHistorySummary.withColumn('InPerson',F.col("totalElections")-F.col("Absentee")-F.col("Provisional")-F.col("Supplemental")) 
# Cache the summarized results
dfVoterHistorySummary = dfVoterHistorySummary.cache()

# Record to test with/validate
dfVoterHistorySummary.filter(dfVoterHistorySummary["Registration Number"] == "07255617").show()

# Show a random sample
dfVoterHistorySummary.show()

# Total # of voters
dfVoterHistorySummary.count()

# Find the voter with the most votes
dfVoterHistorySummary.agg(
    F.max(F.col("totalElections")).alias("totalElections")
).show()
dfVoterHistorySummary.filter(dfVoterHistorySummary["totalElections"] >= 34).show()
dfVoterHistorySummary.groupBy('totalElections').count().show()

dfVoterHistorySummary.filter(dfVoterHistorySummary["totalElections"] > 18).count()

df.filter(df["Registration Number"] == "5821335").show()

# Find the average # of elections per voter
dfVoterHistorySummary.agg(
    F.mean(F.col("totalElections")).alias("totalElections")
).show()

df.count()

import pyspark.pandas as ps
