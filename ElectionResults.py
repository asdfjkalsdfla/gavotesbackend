import findspark
findspark.init()
from pyspark.sql import Window
import pyspark.sql.functions as F


class ElectionResults:
    def __init__(self, spark, election, electionDate, files, fileMapNamesToIDs):
        self.spark = spark
        self.election = election
        self.electionDate = electionDate
        self.dfBase = spark.read.option("header", True).option(
            "inferSchema", True).csv(files)
        self.fileMapNamesToIDs = fileMapNamesToIDs
        self.dfCleansedBaseData = None
        self.summaries = {}

    def cleanseBaseData(self):
        # Set the base data property of the class
       cleansedData = self.dfBase.groupBy(['race', 'county', 'precinct','mode']).pivot("party").sum("votes")
       cleansedData = cleansedData.withColumn("county", F.upper(cleansedData["county"]))
       cleansedData = cleansedData.withColumnRenamed("precinct","electionResultsPrecinctName")
       dfMapCorrections = self.spark.read.option("header", True).option("inferSchema", False).csv(self.fileMapNamesToIDs)
       cleansedData = cleansedData.join(dfMapCorrections, ["county", "electionResultsPrecinctName"], how='left')
       cleansedData = cleansedData.withColumn("precinct", F.upper(cleansedData["precinct"]))
        
       # Cache the value so we don't recompute it with every summary generated
       cleansedData = cleansedData.cache()

       self.dfCleansedBaseData = cleansedData

    def summarizeDataAtLevel(self, level):
        # Check if we have created the base data / if not set the properties
        if (self.dfCleansedBaseData is None):
            self.cleanseBaseData()

        # get the name of the summary
        if len(level) > 0:
            levelName = "_".join(level).replace(" ", "-").lower()
        else:
            levelName = "statewide"

        dfElectionSummarizedByMode = self.dfCleansedBaseData.groupby(['race']+level+['mode']).agg(F.sum("democratic").alias(
            "democratic"), F.sum("republican").alias("republican"), F.sum("other").alias("other"))
        dfElectionCollected = dfElectionSummarizedByMode.groupBy(['race']+level).agg(F.collect_list(F.struct(
            'mode', "democratic", "republican", "other")).alias("resultsByMode"))
        dfElectionSummarized = dfElectionSummarizedByMode.groupby(['race']+level).agg(F.sum("democratic").alias(
            "democratic"), F.sum("republican").alias("republican"), F.sum("other").alias("other"))

        dfElectionSummarized = dfElectionSummarized.join(dfElectionCollected, ['race']+level)
        
        dfElectionSummarized = dfElectionSummarized.groupBy(level).agg(F.collect_list(F.struct("race","democratic","republican","other","resultsByMode")).alias("races"))
        # Save the values
        self.summaries[levelName] = dfElectionSummarized

        return dfElectionCollected

    def summarizeData(self, gbLevels):
        for level in gbLevels:
            self.summarizeDataAtLevel(level)

    def exportSummaries(self):

        for summary in list(self.summaries):
            df = self.summaries[summary]
            print(summary)
            # Really a bad pattern here, but it works for this scale data
            # Using fixed output names for now
            df.coalesce(1).select(F.to_json(F.struct(*df.columns)).alias("json"))\
                .groupBy(F.spark_partition_id())\
                .agg(F.collect_list("json").alias("json_list"))\
                .select(F.col("json_list").cast("string"))\
                .write.mode("overwrite").text("./data/electionResultsSummary/"+self.election+"/"+summary+".json")
