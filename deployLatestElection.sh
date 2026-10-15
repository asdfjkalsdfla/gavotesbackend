#!/bin/bash
set -e
set -x

# elections=( 2016_general 2018_general 2020_general 2021_senate_runoff 2022_general 2022_runoff 2024_primary )
elections=( 2026_general )

levels=(
    "statewide:state"
    "county:county"
    "county_precinct:precinct"
)

for election in "${elections[@]}"
do
    for item in "${levels[@]}"
    do
        level="${item%%:*}"
        suffix="${item##*:}"
        cp -v ./data/absenteeSummary/$election/$level.json/part*.txt ./data/frontend/absentee/absenteeSummary-$election-$suffix.json
        aws s3 cp data/frontend/absentee/absenteeSummary-$election-$suffix.json s3://georgiavotesvisual-latest/static/absentee/absenteeSummary-$election-$suffix.json --cache-control "max-age=3600"
        if [ -d ./data/electionResultsSummary/$election ]; then
            cp -v ./data/electionResultsSummary/$election/$level.json/part*.txt ./data/frontend/electionResults/electionResultsSummary-$election-$suffix.json
            aws s3 cp data/frontend/electionResults/electionResultsSummary-$election-$suffix.json s3://georgiavotesvisual-latest/static/electionResults/electionResultsSummary-$election-$suffix.json --cache-control "max-age=3600"
        fi
    done
done

aws cloudfront create-invalidation --distribution-id E3PTAV7LNU8JS2 --paths "/static/*"
