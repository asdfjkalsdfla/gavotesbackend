#!/usr/bin/env python3
from ElectionResults import ElectionResults
from subprocess import Popen
from AbsenteeBallots import AbsenteeBallots
import findspark
findspark.init()
from pyspark.sql import SparkSession
import json



def main():
    spark = SparkSession.builder.master(
        "local").appName("GAVotesVisual").getOrCreate()

    gbLevels = [["County", "County Precinct"], ['County'], []]
    gbLevels = [["county", "precinct"], ['county'], []]

    with open('elections.json') as f:
        elections = json.load(f)

    for election in elections:

        if (election["absenteeDownload"]):
            process = Popen('./absenteeDataDownload.sh %s %s' % (str(
                election["absenteeElectionNumber"]), str(election["absenteeFileDir"])), shell=True)
            process.wait()
        if (election["absenteeSummarize"]):
            abs = AbsenteeBallots(
                spark, election["name"], election["date"], election["absenteeFiles"])
            abs.summarizeData(gbLevels)
            abs.exportSummaries()
        if (election["resultsDownload"]):
            processResultDownload = Popen('./electionResultsPull.js -e %s -o %s' % (str(
                election["resultsClarityID"]), str(election["resultsDownloadFile"])), shell=True)
            processResultDownload.wait()
        if (election["resultsSummarize"]):
            results = ElectionResults(spark, election["name"], election["date"], election["resultsDownloadFile"])
            results.summarizeData(gbLevels)
            results.exportSummaries()

main()
