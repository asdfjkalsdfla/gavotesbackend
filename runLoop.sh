#!/usr/bin/env bash
set -e
set -x
./electionResultsPull.js -e 116564 -o data/electionResults/2022_runoff/2022_runoff.csv
echo 'US Senate,Bacon,Douglas,"other","other","Absentee by Mail Votes","1"' >> data/electionResults/2022_runoff/2022_runoff.csv
./electionResultPrecinctNamelToID.py 2022_runoff > precinct_changes_2022.csv
./main.py
./copyToFrontEnd.sh