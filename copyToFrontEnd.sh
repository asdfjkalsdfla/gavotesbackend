#!/usr/bin/env bash
# set -e


# rm -rf ./data/frontend
mkdir -p data/frontend/absentee
mkdir -p data/frontend/electionResults
mkdir -p data/frontend/demographics
mkdir -p data/frontend/shapeFiles

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
        if [ -d ./data/electionResultsSummary/$election ]; then
            cp -v ./data/electionResultsSummary/$election/$level.json/part*.txt ./data/frontend/electionResults/electionResultsSummary-$election-$suffix.json
        fi
    done
done

# cp ./data/electionResults/2022_runoff/2022_runoff_map_ids_manual.csv ./data/frontend/2022_general_map_ids_manual.csv
# cp ./data/electionResults/2022_runoff/2022_runoff_map_ids.csv ./data/frontend/2022_general_map_ids.csv

# cp -v ./data/geojson/precincts/2022_simple/GA_precincts_* ./data/frontend/shapeFiles/.
# cp -v ./data/electionResults/2022_runoff/2022_runoff_map_2022_ids_manual.csv ./data/frontend/.
# cp -v ./data/electionResults/2022_runoff/2022_runoff_map_2022_ids.csv ./data/frontend/.
# cp -v ./data/geojson/precincts/2020_simple/demographics-state-2020.json ./data/frontend/demographics/.
# cp -v ./data/geojson/precincts/2020_simple/demographics-county-2020.json ./data/frontend/demographics/.
# cp -v ./data/geojson/precincts/2020_simple/demographics-precinct-2020.json ./data/frontend/demographics/.

exit 0