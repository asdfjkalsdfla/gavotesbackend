#!/usr/bin/env bash
set -e
set -x
node electionResultsPull2024.js -r "President" -o data/electionResults/2024/2024_general.csv -e "2024NovGen"
uv run electionResultPrecinctNamelToID.py 2024_general --mapYear 2022 --raceYear 2024 > precinct_changes_2024.csv
uv run main.py
./copyToFrontEnd.sh
./deploy.sh