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
            process=Popen('./absenteeDataDownload.sh %s %s' % (str(election["electionNumber"]),str(election["absenteeFileDir"])), shell=True)
            process.wait()

        if(election["absenteeSummarize"]):
            abs = AbsenteeBallots(
                spark, election["name"], election["date"], election["absenteeFiles"])
            abs.summarizeData(gbLevels)
            abs.exportSummaries()


main()
