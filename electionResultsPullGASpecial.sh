# !/bin/bash
set -e
set -x

node electionResultsPull.js -e 107556 -s GA -o election_2021_01_05_runoff_precinct.csv
echo `date` >> election_2021_01_05_runoff_precinct.csv
aws s3 cp election_2021_01_05_runoff_precinct.csv s3://georgiavotesvisual/election_2021_01_05_runoff_precinct.csv
python3 electionResultsSummary.py
xsv table election_2021_01_05_runoff_summary.csv