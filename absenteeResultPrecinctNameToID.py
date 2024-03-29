#!/usr/bin/env python3
import pandas as pd
from thefuzz import process
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("year")
parser.add_argument("--mapYear", default="2022",
                    help="specifies the precinct year")
parser.add_argument("--county", help="specifies the county")
args = parser.parse_args()
year = args.year
mapYear = args.mapYear

absenteeResults = pd.read_json('data/absenteeSummary/2022_runoff/county_precinct.json/part-00000-a7434c66-a5bc-44b1-8fbe-2cb4a6b3cc1e-c000.txt')
absenteeResultsCountiesAndPrecincts = absenteeResults[['county', 'precinct']].drop_duplicates()
absenteeResultsCountiesAndPrecincts['county'] = absenteeResults['county'].str.upper()
if args.county:
    absenteeResultsCountiesAndPrecincts = absenteeResultsCountiesAndPrecincts[absenteeResultsCountiesAndPrecincts['county'] == args.county.upper()]
electionResultsCounties = absenteeResultsCountiesAndPrecincts['county'].unique().tolist()


mapCountyPrecinctList = pd.read_csv('data/geojson/precincts/'+mapYear+'_simple/GA_precincts_id_to_name.csv')
mapCountyPrecinctList['county'] = mapCountyPrecinctList['county'].str.upper()
mapCountyPrecinctList['matchLabel'] = mapCountyPrecinctList['precinct']

# manualMapLabelToOverrides = pd.read_csv('data/electionResults/'+year+'/'+year+'_general_map_ids_manual.csv', dtype={'precinct':'string'})

f = open('data/absenteeSummary/'+year+'_runoff/' + year+'_general_map'+mapYear+'_ids.csv', "w")
f.write('county,precinct,absenteePrecinct,score\n')

for county in electionResultsCounties:
    mapPrecincts = mapCountyPrecinctList[mapCountyPrecinctList['county'] == county]
    mapPrecinctsCandidates = mapPrecincts['matchLabel'].tolist()
    precinctsInCounty = absenteeResultsCountiesAndPrecincts[absenteeResultsCountiesAndPrecincts['county']==county]['precinct'].tolist()
    # print(precinctsInCounty)
    mapPrecinctsInCountyID = mapCountyPrecinctList[mapCountyPrecinctList['county']==county]['precinct'].tolist()
    # print(mapPrecinctsInCountyID)
    # print(mapPrecinctsLabels)

    for precinct in precinctsInCounty:
        # overrides = manualMapLabelToOverrides[( (manualMapLabelToOverrides['county']==county) & (manualMapLabelToOverrides['electionResultsPrecinctName']==precinct))]
        # if(overrides.size > 0) :
        #     # precinctID = overrides.at[0, 'precinct']
        #     precinctID = overrides.iat[0, 1]
        #     label = overrides.iat[0, 2]
        #     mapLabelMatched=(label,100)
        if not isinstance(precinct, str) :
            continue

        mapLabelMatched=process.extractOne(precinct, mapPrecinctsCandidates)
        precinctID=mapPrecincts[mapPrecincts['matchLabel']
            == mapLabelMatched[0]]['precinct'].tolist()[0]

        line="{county},{precinctID},\"{precinct}\",{score:.2f}".format(
            county=county, precinctID=precinctID, precinct=precinct, mapLabelMatched=mapLabelMatched[0], score=mapLabelMatched[1])
        if mapLabelMatched[1] > 80:
            f.write(line+"\n")
        if mapLabelMatched[1] < 100:
            print(line)

f.close()
