# import findspark
# findspark.init()
import pyspark.sql.functions as F
from pyspark.sql import Window

class AbsenteeBallots:
    def __init__(self, spark, election, electionDate, files, idOverridesFile=False):
        self.spark = spark
        self.election = election
        self.electionDate = electionDate
        self.dfBase = spark.read.option("header", True).option(
            "inferSchema", False).csv(files)
        self.idOverridesFile=idOverridesFile
        self.dfCleansedBaseData = None
        self.summaries = {}
        

    def cleanseBaseData(self):
        # change column names
        dfCleansed = self.dfBase.withColumnRenamed("County", "county").withColumnRenamed("County Precinct", "precinct")
        # Only look at accepted ballots
        dfAccepted = dfCleansed.filter(dfCleansed["Ballot Status"] == "A")
        
        # Summarize the data from ballot level to precinct level for performance reasons
        dfVotesByDate = dfAccepted.groupby(["county", "precinct", "Ballot Return Date"]).agg(F.count("Voter Registration #").alias("votesOnDate"))
        
        # Convert Ballot Return Date from string to actual date
        dfVotesByDate = dfVotesByDate.withColumn("DateDT", F.to_date("Ballot Return Date", "MM/dd/yyyy"))

        # Cleansed Data
        cleansedData = dfVotesByDate.withColumn("county", F.upper(dfVotesByDate["county"]))
        cleansedData = cleansedData.withColumn("county", F.when(F.col("county").startswith("BENHILL"),"BEN HILL").
                                                when(F.col("county").startswith("JEFFDAVIS"),"JEFF DAVIS").
                                                otherwise(F.col("county")))

        if self.idOverridesFile :
            dfIDCorrections = self.spark.read.option("header", True).option("inferSchema", False).csv(self.idOverridesFile)
            cleansedData = cleansedData.withColumnRenamed("precinct","absenteePrecinct")
            cleansedData = cleansedData.join(dfIDCorrections, ["county", "absenteePrecinct"], how='left')

        cleansedData = cleansedData.withColumn("precinct", F.upper(cleansedData["precinct"]))

        # Cache the value so we don't recompute it with every summary generated
        cleansedData = cleansedData.cache()

        # Set the base data property of the class
        self.dfCleansedBaseData = cleansedData

    def summarizeDataAtLevel(self, level):
        # Check if we have created the base data / if not set the properties
        if(self.dfCleansedBaseData is None) :
            self.cleanseBaseData()

        # get the name of the summary
        if len(level) > 0 : 
            levelName = "_".join(level).replace(" ","-").lower()
        else :
            levelName = "statewide"

        # Group by the summary level            
        dfVotesByDate = self.dfCleansedBaseData.groupby(level + ["DateDT"]).agg(
            F.sum("votesOnDate").alias("votesOnDate"))
        # Add the days from the election
        dfVotesByDate = dfVotesByDate.withColumn(
            "DaysFromElection", F.datediff("DateDT", F.to_date(F.lit(self.electionDate))))
        
        # Calculate the votes to date (window functions for the win)
        dfTotalVotesToDateByDate = dfVotesByDate.withColumn("votesToDate", F.sum("votesOnDate").over(
            Window.partitionBy(level).orderBy(dfVotesByDate.DaysFromElection))).cache()

        # Create a summary level and then days as an array off that
        dfCollected = dfTotalVotesToDateByDate.groupBy(level).agg(F.collect_list(F.struct(
            'DaysFromElection', 'DateDT', 'votesToDate', 'votesOnDate')).alias("votesByDay"))

        # dfCollected.show(10)

        # Save the values
        self.summaries[levelName] = dfCollected

        return dfCollected

    def summarizeData(self, gbLevels):
        for level in gbLevels:
            self.summarizeDataAtLevel(level)
            
    def exportSummaries(self) : 
        for summary in list(self.summaries) :
            df = self.summaries[summary]
            # Really a bad pattern here, but it works for this scale data
            # Using fixed output names for now
            df.coalesce(1).select(F.to_json(F.struct(*df.columns)).alias("json"))\
                .groupBy(F.spark_partition_id())\
                .agg(F.collect_list("json").alias("json_list"))\
                .select(F.col("json_list").cast("string"))\
                .write.mode("overwrite").text("./data/absenteeSummary/"+self.election+"/"+summary+".json")