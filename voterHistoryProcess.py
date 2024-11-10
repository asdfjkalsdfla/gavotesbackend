# import findspark
# findspark.init()

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import pyspark.pandas as ps
#.config("spark.executor.memory", "8g")

spark = SparkSession.builder\
    .master("local")\
    .appName("GAVotesVisual")\
    .master("local[6]")\
    .getOrCreate()

##############################################
# Convert Absentee Data to Voter
##############################################
schemaVoterInfo = StructType([StructField("CountyName", StringType(), True)]) \
    .add("Voter Registration #",IntegerType(),True) \
    .add("Last Name",StringType())\
    .add("First Name",StringType())\
    .add("Middle Name",StringType(),True)\
    .add("Suffix",StringType(),True) \
    .add("Street #",StringType()) \
    .add("Street Name",StringType(),True)\
    .add("Apt/Unit",StringType(),True)\
    .add("City",StringType(),True)\
    .add("State",StringType(),True)\
    .add("Zip Code",StringType())

elections = [
    { "election" : "2024", "priority": 2024.1 },
    { "election" : "2024_primary", "priority": 2024 },
    { "election" : "2023", "priority": 2023 },
    { "election" : "2022_runoff", "priority": 2022.1 },
    { "election" : "2022", "priority": 2022 }, 
    { "election" : "2021", "priority": 2021 }, 
    { "election" : "2020", "priority": 2020 }, 
    { "election" : "2018", "priority": 2018 }, 
    { "election" : "2016", "priority": 2016 }, 
]

for election in elections :
    dfVoterInfoFromAbsenteeNew = spark.read.options(delimiter=",", header=True,dateFormat="yyyyMMdd", ignoreTrailingWhiteSpace=True).\
        schema(schemaVoterInfo).csv("data/absentee/{}/data/*.csv".format(election.get('election')))
    # dfVoterInfoFromAbsenteeNew = spark.read.\
    #     csv("data/absentee/{}/data/*.csv".format(election.get('election')))
    dfVoterInfoFromAbsenteeNew = dfVoterInfoFromAbsenteeNew.withColumn("absenteeDataYear", F.lit(election.get('priority')))
    if 'dfVoterInfoFromAbsenteeAll' in locals() : 
        dfVoterInfoFromAbsenteeAll = dfVoterInfoFromAbsenteeNew.unionByName(dfVoterInfoFromAbsenteeAll)
    else :
        dfVoterInfoFromAbsenteeAll = dfVoterInfoFromAbsenteeNew

dfVoterInfoFromAbsenteeAll = dfVoterInfoFromAbsenteeAll.withColumnRenamed("Voter Registration #","id")
dfVoterInfoFromAbsenteeAll = dfVoterInfoFromAbsenteeAll.withColumnRenamed("First Name","firstName")
dfVoterInfoFromAbsenteeAll = dfVoterInfoFromAbsenteeAll.withColumnRenamed("Middle Name","middleName")
dfVoterInfoFromAbsenteeAll = dfVoterInfoFromAbsenteeAll.withColumnRenamed("Last Name","lastName")
dfVoterInfoFromAbsenteeAll = dfVoterInfoFromAbsenteeAll.withColumnRenamed("Street #","streetNumber")
dfVoterInfoFromAbsenteeAll = dfVoterInfoFromAbsenteeAll.withColumnRenamed("Street Name","streetName")
dfVoterInfoFromAbsenteeAll = dfVoterInfoFromAbsenteeAll.withColumnRenamed("City","city")
dfVoterInfoFromAbsenteeAll = dfVoterInfoFromAbsenteeAll.withColumnRenamed("State","state")
dfVoterInfoFromAbsenteeAll = dfVoterInfoFromAbsenteeAll.withColumnRenamed("Zip Code","zip")
dfVoterInfoFromAbsenteeAll = dfVoterInfoFromAbsenteeAll.withColumnRenamed("CountyName","countyCurrent")

dfVoterInfoFromAbsenteeAll = dfVoterInfoFromAbsenteeAll.withColumn("firstName", F.initcap(F.col('firstName'))) \
    .withColumn("middleName", F.initcap(F.col('middleName'))) \
    .withColumn("lastName", F.initcap(F.col('lastName'))) \
    .withColumn("streetName", F.initcap(F.col('streetName'))) \
    .withColumn("city", F.initcap(F.col('city')))\
    .withColumn("zip", F.initcap(F.col('zip')))\
    .withColumn("countyCurrent", F.initcap(F.col('countyCurrent')))

dfVoterInfoFromAbsenteeLatestYear = dfVoterInfoFromAbsenteeAll.groupby('id').agg(F.max(F.col("absenteeDataYear")).alias("absenteeDataYear"))

dfVoterInfoFromAbsentee = dfVoterInfoFromAbsenteeLatestYear.join(dfVoterInfoFromAbsenteeAll, ["id", "absenteeDataYear"], how='inner')
dfVoterInfoFromAbsentee = dfVoterInfoFromAbsentee.distinct()
dfVoterInfoFromAbsentee.write.mode("overwrite").partitionBy("countyCurrent").parquet("data/votehistory/dataVoters.parquet")

exit()

#############################################
# Convert to parquet (format up to 2022)
#############################################
schemaVoterHistory = StructType() \
    .add("County Number",IntegerType(),True) \
    .add("Registration Number",IntegerType(),True) \
    .add("Election Date",DateType(),True) \
    .add("Election Type",IntegerType(),True) \
    .add("Party",StringType(),True) \
    .add("Absentee",StringType(),True)\
    .add("Provisional",StringType(),True)\
    .add("Supplemental",StringType(),True)

df = spark.read.options(delimiter=",", header=True,dateFormat="yyyyMMdd", ignoreTrailingWhiteSpace=True).schema(schemaVoterHistory).csv("data/votehistory/data/*.csv")

df = df.withColumn("Absentee", F.when(F.col('Absentee') == "Y",True).otherwise(False))
df = df.withColumn('Provisional', F.when(F.col('Provisional') == "Y",True).otherwise(False))
df = df.withColumn('Supplemental', F.when(F.col('Supplemental') == "Y",True).otherwise(False))
# df.printSchema()

df = df.withColumnRenamed("Registration Number","id")
df = df.withColumnRenamed("County Number","county")
df = df.withColumnRenamed("Election Date","election")
df = df.withColumnRenamed("Party","party")
df = df.withColumnRenamed("Absentee","absentee")
df = df.withColumnRenamed("Provisional","provisional")
df = df.withColumnRenamed("Supplemental","supplemental")

df.write.mode("overwrite").partitionBy("election").parquet("data/votehistory/data.parquet")
# exit()

#############################################
# Convert to parquet (2023 format)
#############################################
schemaVoterHistory = StructType() \
     .add("County Name",StringType(),True) \
     .add("Voter Registration Number",IntegerType(),True) \
     .add("Election Date",DateType(),True) \
     .add("Election Type",StringType(),True) \
     .add("Party",StringType(),True) \
     .add("Ballot Style",StringType(),True) \
     .add("Absentee",StringType(),True)\
     .add("Provisional",StringType(),True)\
     .add("Supplemental",StringType(),True)

df = spark.read.options(delimiter=",", header=True,dateFormat="MM/dd/yyyy", ignoreTrailingWhiteSpace=True).schema(schemaVoterHistory).csv("data/votehistory/2024.csv")

df = df.withColumn("Absentee", F.when(F.col('Absentee') == "Y",True).otherwise(False))
df = df.withColumn('Provisional', F.when(F.col('Provisional') == "Y",True).otherwise(False))
df = df.withColumn('Supplemental', F.when(F.col('Supplemental') == "Y",True).otherwise(False))

df = df.withColumn('Party', F.when(F.col('Party') == "DEMOCRAT","D").when(F.col('Party') == "REPUBLICAN","R").otherwise(""))
# # df.printSchema()

df = df.withColumnRenamed("Voter Registration Number","id")
# df = df.withColumnRenamed("County Number","county")
df = df.withColumnRenamed("Election Date","election")
df = df.withColumnRenamed("Party","party")
df = df.withColumnRenamed("Absentee","absentee")
df = df.withColumnRenamed("Provisional","provisional")
df = df.withColumnRenamed("Supplemental","supplemental")
df = df.withColumnRenamed("Election Type","Election Type Description")

df.write.mode("append").partitionBy("election").parquet("data/votehistory/data.parquet")
exit()
