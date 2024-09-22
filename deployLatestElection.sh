#!/bin/bash
set -e
set -x

# elections=( 2016_general 2018_general 2020_general 2021_senate_runoff 2022_general 2022_runoff 2024_primary )
elections=( 2024_general )

declare -A levels
levels["statewide"]="state"
levels["county"]="county"
levels["county_precinct"]="precinct"


for election in "${elections[@]}"
do
    for level in "${!levels[@]}"
    do
        aws s3 cp data/frontend/absentee/absenteeSummary-$election-${levels[$level]}.json s3://georgiavotesvisual-latest/static/absentee/absenteeSummary-$election-${levels[$level]}.json
        cp -v ./data/absenteeSummary/$election/$level.json/part*.txt ./data/frontend/absentee/
        if [ -d ./data/electionResultsSummary/$election ]; then
            aws s3 cp data/frontend/electionResults/electionResultsSummary-$election-${levels[$level]}.json s3://georgiavotesvisual-latest/static/electionResults/electionResultsSummary-$election-${levels[$level]}.json
        fi
    done
done

aws cloudfront create-invalidation --distribution-id E3PTAV7LNU8JS2 --paths "/static/absentee/absenteeSummary*"