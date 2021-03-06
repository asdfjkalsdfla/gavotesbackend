import findspark
findspark.init()

import json
from pyspark.sql import SparkSession
from AbsenteeBallots import AbsenteeBallots
from subprocess import Popen


def main():
    spark = SparkSession.builder.master(
        "local").appName("GAVotesVisual").getOrCreate()

    gbLevels = [["County", "County Precinct"], ['County'], []]

    with open('elections.json') as f:
        elections = json.load(f)

    for election in elections:

        if(election["absenteeDownload"]):
            process=Popen('./absenteeDataDownload.sh %s %s' % (str(election["absenteeElectionNumber"]),str(election["absenteeFileDir"])), shell=True)
            process.wait()

        if(election["absenteeSummarize"]):
            abs = AbsenteeBallots(
                spark, election["name"], election["date"], election["absenteeFiles"])
            abs.summarizeData(gbLevels)
            abs.exportSummaries()
        if(election["resultsDownload"]):
            processResultDownload=Popen('./electionResultsPull.js -e %s -o %s' % (str(election["resultsClarityID"]),str(election["resultsDownloadFile"])), shell=True)
            processResultDownload.wait()


main()
