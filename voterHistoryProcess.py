from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import pyspark.pandas as ps


def create_spark_session():
    return SparkSession.builder \
        .master("local[6]") \
        .appName("GAVotesVisual") \
        .getOrCreate()


def build_voter_profiles_from_absentee(spark):
    schema = StructType([StructField("CountyName", StringType(), True)]) \
        .add("Voter Registration #", IntegerType(), True) \
        .add("Last Name", StringType()) \
        .add("First Name", StringType()) \
        .add("Middle Name", StringType(), True) \
        .add("Suffix", StringType(), True) \
        .add("Street #", StringType()) \
        .add("Street Name", StringType(), True) \
        .add("Apt/Unit", StringType(), True) \
        .add("City", StringType(), True) \
        .add("State", StringType(), True) \
        .add("Zip Code", StringType())

    elections = [
        {"election": "2024", "priority": 2024.1},
        {"election": "2024_primary", "priority": 2024},
        {"election": "2023", "priority": 2023},
        {"election": "2022_runoff", "priority": 2022.1},
        {"election": "2022", "priority": 2022},
        {"election": "2021", "priority": 2021},
        {"election": "2020", "priority": 2020},
        {"election": "2018", "priority": 2018},
        {"election": "2016", "priority": 2016},
    ]

    dfAll = None
    for election in elections:
        dfNew = spark.read.options(delimiter=",", header=True, dateFormat="yyyyMMdd", ignoreTrailingWhiteSpace=True) \
            .schema(schema).csv(f"data/absentee/{election['election']}/data/*.csv")
        dfNew = dfNew.withColumn("absenteeDataYear", F.lit(election["priority"]))
        if dfAll is None:
            dfAll = dfNew
        else:
            dfAll = dfNew.unionByName(dfAll)

    dfAll = dfAll.withColumnsRenamed({
        "Voter Registration #": "id",
        "First Name": "firstName",
        "Middle Name": "middleName",
        "Last Name": "lastName",
        "Street #": "streetNumber",
        "Street Name": "streetName",
        "City": "city",
        "State": "state",
        "Zip Code": "zip",
        "CountyName": "countyCurrent",
    })

    dfAll = dfAll.withColumns({
        "firstName": F.initcap(F.col("firstName")),
        "middleName": F.initcap(F.col("middleName")),
        "lastName": F.initcap(F.col("lastName")),
        "streetName": F.initcap(F.col("streetName")),
        "city": F.initcap(F.col("city")),
        "zip": F.initcap(F.col("zip")),
        "countyCurrent": F.initcap(F.col("countyCurrent")),
    })

    dfLatestYear = dfAll.groupby("id").agg(F.max(F.col("absenteeDataYear")).alias("absenteeDataYear"))
    dfVoterInfoFromAbsentee = dfLatestYear.join(dfAll, ["id", "absenteeDataYear"], how="inner").distinct()
    dfVoterInfoFromAbsentee.write.mode("overwrite").partitionBy("countyCurrent").parquet("data/votehistory/dataVoters.parquet")


def load_voter_history_pre2023_to_parquet(spark):
    schema = StructType() \
        .add("County Number", IntegerType(), True) \
        .add("Registration Number", IntegerType(), True) \
        .add("Election Date", DateType(), True) \
        .add("Election Type", IntegerType(), True) \
        .add("Party", StringType(), True) \
        .add("Absentee", StringType(), True) \
        .add("Provisional", StringType(), True) \
        .add("Supplemental", StringType(), True)

    df = spark.read.options(delimiter=",", header=True, dateFormat="yyyyMMdd", ignoreTrailingWhiteSpace=True) \
        .schema(schema).csv("data/votehistory/data/*.csv")

    df = df.withColumns({
        "Absentee": F.when(F.col("Absentee") == "Y", True).otherwise(False),
        "Provisional": F.when(F.col("Provisional") == "Y", True).otherwise(False),
        "Supplemental": F.when(F.col("Supplemental") == "Y", True).otherwise(False),
    }).withColumnsRenamed({
        "Registration Number": "id",
        "County Number": "county",
        "Election Date": "election",
        "Party": "party",
        "Absentee": "absentee",
        "Provisional": "provisional",
        "Supplemental": "supplemental",
    })

    df.write.mode("overwrite").partitionBy("election").parquet("data/votehistory/data.parquet")


def load_voter_history_2023_to_parquet(spark):
    schema = StructType() \
        .add("County Name", StringType(), True) \
        .add("Voter Registration Number", IntegerType(), True) \
        .add("Election Date", DateType(), True) \
        .add("Election Type", StringType(), True) \
        .add("Party", StringType(), True) \
        .add("Ballot Style", StringType(), True) \
        .add("Absentee", StringType(), True) \
        .add("Provisional", StringType(), True) \
        .add("Supplemental", StringType(), True)

    df = spark.read.options(delimiter=",", header=True, dateFormat="MM/dd/yyyy", ignoreTrailingWhiteSpace=True) \
        .schema(schema).csv("data/votehistory/2024.csv")

    df = df.withColumns({
        "Absentee": F.when(F.col("Absentee") == "Y", True).otherwise(False),
        "Provisional": F.when(F.col("Provisional") == "Y", True).otherwise(False),
        "Supplemental": F.when(F.col("Supplemental") == "Y", True).otherwise(False),
        "Party": F.when(F.col("Party") == "DEMOCRAT", "D").when(F.col("Party") == "REPUBLICAN", "R").otherwise(""),
    }).withColumnsRenamed({
        "Voter Registration Number": "id",
        "Election Date": "election",
        "Party": "party",
        "Absentee": "absentee",
        "Provisional": "provisional",
        "Supplemental": "supplemental",
        "Election Type": "Election Type Description",
    })

    df.write.mode("append").partitionBy("election").parquet("data/votehistory/data.parquet")


def build_voter_history(spark):
    dfVoterInfoFromAbsentee = spark.read.parquet("data/votehistory/dataVoters.parquet")
    df = spark.read.parquet("data/votehistory/data.parquet")

    dfWithAllGrouped = df.groupBy("id").agg(
        F.collect_list(F.struct("election", "county", "party", "absentee", "provisional", "supplemental")).alias("voterHistory")
    ).cache()

    dfWithAll = dfVoterInfoFromAbsentee.join(dfWithAllGrouped, "id", how="left")
    dfWithAll.write.mode("overwrite").json("./data/voterHistory/test.json")

    dfLastVoted = df.groupby("id").agg(F.max(F.col("election")).alias("electionLastVoted"))
    dfVoterWithLast = dfVoterInfoFromAbsentee.join(dfLastVoted, "id", how="left")

    dfVotersByCityStreet = dfVoterWithLast.groupBy(["countyCurrent", "city", "streetName"]).agg(
        F.collect_list(F.struct("id", "firstName", "lastName", "electionLastVoted")).alias("voters")
    )
    dfVotersByCityStreet.write.mode("overwrite").json("./data/voterHistory/county_city_street.json")


def summarize_voter_history(spark):
    cnt_cond = lambda cond: F.sum(F.when(cond, 1).otherwise(0))

    dfVoterInfoFromAbsentee = spark.read.parquet("data/votehistory/dataVoters.parquet")
    df = spark.read.parquet("data/votehistory/data.parquet")
    df = dfVoterInfoFromAbsentee.join(df, "id", how="left")

    dfSummary = df.groupBy("id").agg(
        F.count(F.col("id")).alias("totalElections"),
        cnt_cond(F.col("party") == "D").alias("primaryElectionsD"),
        cnt_cond(F.col("party") == "R").alias("primaryElectionsR"),
        cnt_cond(F.col("party") == "NP").alias("primaryElectionsNP"),
        F.sum(F.col("absentee").cast("long")).alias("absentee"),
        F.sum(F.col("provisional").cast("long")).alias("provisional"),
        F.sum(F.col("supplemental").cast("long")).alias("supplemental"),
        F.max(F.col("election")).alias("lastElection"),
    )
    dfSummary = dfSummary.withColumn(
        "inPerson",
        F.col("totalElections") - F.col("absentee") - F.col("provisional") - F.col("supplemental")
    ).cache()

    dfSummary.filter(dfSummary["lastElection"] > "2024-01-01").show()
    dfSummary.agg(F.mean(F.col("totalElections")).alias("totalElections")).show()


def search_voter(last_name, first_name, city):
    dfVoterInfoFromAbsentee = ps.read_parquet("data/votehistory/dataVoters.parquet", index_col="id")
    result = dfVoterInfoFromAbsentee[
        (dfVoterInfoFromAbsentee["lastName"] == last_name) &
        (dfVoterInfoFromAbsentee["firstName"] == first_name) &
        (dfVoterInfoFromAbsentee["city"] == city)
    ]
    print(result.head(10))


def pull_election_history():
    dfVoterHistory = ps.read_parquet("data/votehistory/data.parquet")
    dfVoterElectionTypes = ps.read_csv("data/votehistory/codes.txt")

    votersByElection = dfVoterHistory.groupby("election").agg(
        voters=("id", "count"),
        electionType=("Election Type", "min"),
    ).reset_index()
    votersByElection = votersByElection.rename(columns={"election": "id"})
    votersByElection["name"] = votersByElection["id"]
    votersByElection["electionType"] = votersByElection["electionType"].astype("int")
    votersByElection = ps.merge(votersByElection, dfVoterElectionTypes, how="left", on="electionType")
    votersByElection.to_json("data/voterHistory/elections.json")


if __name__ == "__main__":
    spark = create_spark_session()
    search_voter("Test", "Test", "Test")
