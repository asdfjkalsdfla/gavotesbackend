from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import pyspark.pandas as ps
import glob
import os
import shutil

VOTER_PROFILES_OUTPUT = "data/votehistory/dataVoters.parquet"
VOTER_PROFILES_OUTPUT_TMP = "data/votehistory/dataVoters_tmp.parquet"


def create_spark_session():
    return SparkSession.builder \
        .master("local[6]") \
        .appName("GAVotesVisual") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.maxResultSize", "4g") \
        .getOrCreate()
        # .config("spark.sql.parquet.enableVectorizedReader", "false") \
        



def build_voter_profiles_from_absentee(spark, incremental=False):
    """Build voter profiles from the absentee data files.

    When incremental=True, only elections marked "new": True in the
    elections list below are read, and the result is merged with the
    existing VOTER_PROFILES_OUTPUT parquet instead of reprocessing every
    election from scratch.
    """
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

    # Set "new": True on an election entry to mark its absentee files as not
    # yet incorporated into VOTER_PROFILES_OUTPUT. When incremental=True, only
    # elections marked "new" are read from disk; everything else is assumed to
    # already be reflected in the existing parquet output.
    elections = [
        {"election": "2026_primary_runoff", "priority": 2026.1, "new": True},
        {"election": "2026_primary", "priority": 2026.0, "new": True},
        {"election": "2025_general", "priority": 2025, "new": True},
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
        if incremental and not election.get("new", False):
            continue
        files = sorted(glob.glob(f"data/absentee/{election['election']}/data/*.csv"))
        if not files:
            continue
        dfNew = spark.read.options(delimiter=",", header=True, dateFormat="yyyyMMdd", ignoreTrailingWhiteSpace=True) \
            .schema(schema).csv(files)
        dfNew = dfNew.withColumn("absenteeDataYear", F.lit(election["priority"]))
        if dfAll is None:
            dfAll = dfNew
        else:
            dfAll = dfNew.unionByName(dfAll)

    if incremental and dfAll is None:
        print("No new absentee files found since last run, skipping voter profile rebuild.")
        return

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

    if incremental and os.path.exists(VOTER_PROFILES_OUTPUT):
        dfExisting = spark.read.parquet(VOTER_PROFILES_OUTPUT)
        dfAll = dfAll.unionByName(dfExisting)

    dfLatestYear = dfAll.groupby("id").agg(F.max(F.col("absenteeDataYear")).alias("absenteeDataYear"))
    dfVoterInfoFromAbsentee = dfLatestYear.join(dfAll, ["id", "absenteeDataYear"], how="inner").distinct()

    if incremental and os.path.exists(VOTER_PROFILES_OUTPUT):
        # Write to a temp location first since we're reading from
        # VOTER_PROFILES_OUTPUT above; overwriting it directly would corrupt
        # the source data mid-read.
        if os.path.exists(VOTER_PROFILES_OUTPUT_TMP):
            shutil.rmtree(VOTER_PROFILES_OUTPUT_TMP)
        dfVoterInfoFromAbsentee.write.mode("overwrite").partitionBy("countyCurrent").parquet(VOTER_PROFILES_OUTPUT_TMP)
        shutil.rmtree(VOTER_PROFILES_OUTPUT)
        shutil.move(VOTER_PROFILES_OUTPUT_TMP, VOTER_PROFILES_OUTPUT)
    else:
        dfVoterInfoFromAbsentee.write.mode("overwrite").partitionBy("countyCurrent").parquet(VOTER_PROFILES_OUTPUT)


def load_voter_history_pre2023_to_parquet(spark):
    expectedColumns = [
        "County Number", "Registration Number", "Election Date", "Election Type",
        "Party", "Absentee", "Provisional", "Supplemental",
    ]

    # Reading the whole glob in one shot forces Spark to bind every file's
    # rows to a single shared column order (taken from whichever file it
    # samples first) *positionally*, regardless of that file's own header
    # order. Files here don't all use the same physical column order, so we
    # read + normalize each file individually (each file's own header
    # correctly determines its own column order) and union the results.
    files = sorted(glob.glob("data/votehistory/data/*.csv"))
    dfAll = None
    for file in files:
        dfFile = spark.read.options(delimiter=",", header=True, enforceSchema=False, ignoreTrailingWhiteSpace=True) \
            .csv(file)

        missing = [c for c in expectedColumns if c not in dfFile.columns]
        if missing:
            raise ValueError(f"{file} is missing expected column(s): {missing}")

        dfFile = dfFile.select(
            F.col("County Number").cast(IntegerType()).alias("county"),
            F.col("Registration Number").cast(IntegerType()).alias("id"),
            F.to_date(F.col("Election Date"), "yyyyMMdd").alias("election"),
            F.col("Election Type").cast(IntegerType()).alias("Election Type"),
            F.col("Party").alias("party"),
            (F.col("Absentee") == "Y").alias("absentee"),
            (F.col("Provisional") == "Y").alias("provisional"),
            (F.col("Supplemental") == "Y").alias("supplemental"),
        )
        dfAll = dfFile if dfAll is None else dfAll.unionByName(dfFile)

    dfAll.write.mode("overwrite").partitionBy("election").parquet("data/votehistory/data.parquet")


def load_voter_history_2023_to_parquet(spark):
    expectedColumns = [
        "County Name", "Voter Registration Number", "Election Date", "Election Type",
        "Party", "Ballot Style", "Absentee", "Provisional", "Supplemental",
    ]

    # See load_voter_history_pre2023_to_parquet: reading the whole glob at
    # once binds every file's rows to one shared column order positionally,
    # which silently corrupts files whose header order differs (e.g. some
    # files here have "Election Date"/"County Name" swapped). Read + normalize
    # each file individually instead.
    files = sorted(glob.glob("data/votehistory/data/*.csv"))
    dfAll = None
    for file in files:
        dfFile = spark.read.options(delimiter=",", header=True, enforceSchema=False, ignoreTrailingWhiteSpace=True) \
            .csv(file)

        missing = [c for c in expectedColumns if c not in dfFile.columns]
        if missing:
            raise ValueError(f"{file} is missing expected column(s): {missing}")

        dfFile = dfFile.select(
            F.col("County Name").alias("County Name"),
            F.col("Voter Registration Number").cast(IntegerType()).alias("id"),
            F.to_date(F.col("Election Date"), "MM/dd/yyyy").alias("election"),
            F.col("Election Type").alias("Election Type Description"),
            F.when(F.col("Party") == "DEMOCRAT", "D").when(F.col("Party") == "REPUBLICAN", "R").otherwise("").alias("party"),
            F.col("Ballot Style").alias("Ballot Style"),
            (F.col("Absentee") == "Y").alias("absentee"),
            (F.col("Provisional") == "Y").alias("provisional"),
            (F.col("Supplemental") == "Y").alias("supplemental"),
        )
        dfAll = dfFile if dfAll is None else dfAll.unionByName(dfFile)

    dfAll.write.mode("append").partitionBy("election").parquet("data/votehistory/data.parquet")


def build_voter_history(spark):
    dfVoterInfoFromAbsentee = spark.read.parquet("data/votehistory/dataVoters.parquet")
    df = spark.read.parquet("data/votehistory/data.parquet")

    dfWithAllGrouped = df.groupBy("id").agg(
        F.collect_list(F.struct("election", "county", "party", "absentee", "provisional", "supplemental")).alias("voterHistory")
    ).cache()

    dfWithAll = dfVoterInfoFromAbsentee.join(dfWithAllGrouped, "id", how="left")
    # dfWithAll.write.mode("overwrite").json("./data/voterHistory/test.json")

    # Generate DynamoDB JSON format compressed with Gzip
    history_item = F.struct(
        F.struct(
            F.struct(F.col("h.election").cast("string").alias("S")).alias("election"),
            F.struct(F.col("h.county").cast("string").alias("N")).alias("county"),
            F.struct(F.col("h.party").alias("S")).alias("party"),
            F.struct(F.col("h.absentee").alias("BOOL")).alias("absentee"),
            F.struct(F.col("h.provisional").alias("BOOL")).alias("provisional"),
            F.struct(F.col("h.supplemental").alias("BOOL")).alias("supplemental"),
        ).alias("M")
    )

    item_struct = F.struct(
        F.struct(F.col("id").cast("string").alias("N")).alias("id"),
        F.struct(F.col("absenteeDataYear").cast("string").alias("N")).alias("absenteeDataYear"),
        F.struct(F.col("firstName").alias("S")).alias("firstName"),
        F.struct(F.col("middleName").alias("S")).alias("middleName"),
        F.struct(F.col("lastName").alias("S")).alias("lastName"),
        F.struct(F.col("streetNumber").alias("S")).alias("streetNumber"),
        F.struct(F.col("streetName").alias("S")).alias("streetName"),
        F.struct(F.col("city").alias("S")).alias("city"),
        F.struct(F.col("state").alias("S")).alias("state"),
        F.struct(F.col("zip").alias("S")).alias("zip"),
        F.struct(F.col("countyCurrent").alias("S")).alias("countyCurrent"),
        F.struct(F.transform(F.col("voterHistory"), lambda h: history_item)).alias("voterHistory"),
    )

    dfDynamo = dfWithAll.select(F.struct(item_struct.alias("Item")).alias("Item"))
    dfDynamo.write.mode("overwrite").option("compression", "gzip").json("./data/voterHistory/test_dynamodb.json.gz")


    dfLastVoted = df.groupby("id").agg(F.max(F.col("election")).alias("electionLastVoted"))
    dfVoterWithLast = dfVoterInfoFromAbsentee.join(dfLastVoted, "id", how="left")

    dfVotersByCityStreet = dfVoterWithLast.groupBy(["countyCurrent", "city", "streetName"]).agg(
        F.collect_list(F.struct("id", "firstName", "lastName", "electionLastVoted")).alias("voters")
    )
    dfVotersByCityStreet.write.mode("overwrite").json("./data/voterHistory/county_city_street.json")

    dfStreetsByCity = dfVoterWithLast.groupBy(["countyCurrent", "city"]).agg(
        F.collect_set(F.col("streetName")).alias("streets")
    )
    dfStreetsByCity.write.mode("overwrite").json("./data/voterHistory/county_city.json")


def summarize_voter_history(spark):
    cnt_cond = lambda cond: F.sum(F.when(cond, 1).otherwise(0))

    with open("excludes.txt") as f:
        excludeIds = [int(line.strip()) for line in f if line.strip()]

    dfVoterInfoFromAbsentee = spark.read.parquet("data/votehistory/dataVoters.parquet")
    dfVoterInfoFromAbsentee = dfVoterInfoFromAbsentee.filter(~F.col("id").isin(excludeIds))
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
    # build_voter_profiles_from_absentee(spark, incremental=True)
    # load_voter_history_2023_to_parquet(spark)
    build_voter_history(spark)
    # summarize_voter_history(spark)
    # search_voter("Test", "Test", "Test")
