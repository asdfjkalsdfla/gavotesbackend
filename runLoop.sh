#!/usr/bin/env bash
set -e
set -x
./electionResultsPull.js -e 116564 -o data/electionResults/2022_runoff/2022_runoff.csv
./electionResultPrecinctNamelToID.py 2022_runoff --mapYear 2022 > precinct_changes_2022.csv
./main.py
./copyToFrontEnd.sh
./deploy.sh