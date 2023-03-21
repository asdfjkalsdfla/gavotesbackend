#!/usr/bin/env python3
import os
import pandas as pd
from thefuzz import process
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("race")
parser.add_argument("--raceYear", help="the year of the election")
parser.add_argument("--mapYear", default="2022",
                    help="specifies the precinct year")
parser.add_argument("--county", help="specifies the county")
args = parser.parse_args()
race = args.race
if isinstance(args.raceYear, str):
    raceYear = args.raceYear
else:
    raceYear = race

mapYear = args.mapYear


# Get Precincts from Election Results
electionResultsFileLoc = 'data/electionResults/'+raceYear+'/'+race+'.csv'
if not (os.path.isfile(electionResultsFileLoc)):
    print("Election File Doesn't Exist")
    exit(-1)

electionResults = pd.read_csv(electionResultsFileLoc)

electionResultsCountiesAndPrecincts = electionResults[[
    'county', 'precinct']].drop_duplicates()
electionResultsCountiesAndPrecincts['county'] = electionResults['county'].str.upper(
)
if args.county:
    electionResultsCountiesAndPrecincts = electionResultsCountiesAndPrecincts[
        electionResultsCountiesAndPrecincts['county'] == args.county.upper()]
electionResultsCounties = electionResultsCountiesAndPrecincts['county'].unique(
).tolist()

# Get Precincts from Map
mapPrecinctsFileLoc = 'data/geojson/precincts/' + \
    mapYear+'_simple/GA_precincts_id_to_name.csv'
if not (os.path.isfile(mapPrecinctsFileLoc)):
    print("Map Precinct File Doesn't Exist")
    exit(-1)
mapCountyPrecinctList = pd.read_csv(mapPrecinctsFileLoc)
mapCountyPrecinctList['county'] = mapCountyPrecinctList['county'].str.upper()
mapCountyPrecinctList['matchLabel'] = mapCountyPrecinctList['precinctName']

# Get Manually Set Map Between Election Result ID and Map ID
manualLinkElectionResultToMapIDLoc = 'data/electionResults/' + \
    raceYear+'/'+race+'_map_'+mapYear+'_ids_manual.csv'
if os.path.isfile(manualLinkElectionResultToMapIDLoc):
    manualMapLabelToOverrides = pd.read_csv(
        manualLinkElectionResultToMapIDLoc, dtype={'precinct': 'string'})
else:
    manualMapLabelToOverrides = False

f = open('data/electionResults/'+raceYear+'/' +
         race+'_map_'+mapYear+'_ids.csv', "w")
f.write('county,precinct,mapPrecinctName,electionResultsPrecinctName,score\n')

for county in electionResultsCounties:
    mapPrecincts = mapCountyPrecinctList[mapCountyPrecinctList['county'] == county]
    mapPrecinctsLabels = mapPrecincts['matchLabel'].tolist()
    precinctsInCounty = electionResultsCountiesAndPrecincts[
        electionResultsCountiesAndPrecincts['county'] == county]['precinct'].tolist()
    # print(precinctsInCounty)
    mapPrecinctsInCountyID = mapCountyPrecinctList[mapCountyPrecinctList['county'] == county]['precinct'].tolist(
    )
    # print(mapPrecinctsInCountyID)
    # print(mapPrecinctsLabels)

    for precinct in precinctsInCounty:
        overrides = manualMapLabelToOverrides[((manualMapLabelToOverrides['county'] == county) & (
            manualMapLabelToOverrides['electionResultsPrecinctName'] == precinct))] if manualMapLabelToOverrides else False
        if ((overrides) and (overrides.size > 0)):
            # precinctID = overrides.at[0, 'precinct']
            precinctID = overrides.iat[0, 1]
            label = overrides.iat[0, 2]
            mapLabelMatched = (label, 100)
        elif county == "CHATHAM" or county == "WARE" or county == "FORSYTH":
            if (race == "2018" or race == "2016"):
                precinctID = precinct.split(" ")[0]+"C"
            else:
                precinctID = precinct.split(" ")[0]
            labelList = mapPrecincts[mapPrecincts['precinct']
                                     == precinctID]['precinctName'].tolist()
            if len(labelList) > 0:
                label = labelList[0]
                mapLabelMatched = (label, 100)
            else:
                label = "??"
                mapLabelMatched = (label, 0)
        elif county == "RICHMOND" and race == "2022":
            precinctID = precinct
            labelList = mapPrecincts[mapPrecincts['precinct']
                                     == precinctID]['precinctName'].tolist()
            if len(labelList) > 0:
                label = labelList[0]
                mapLabelMatched = (label, 100)
            else:
                label = "??"
                mapLabelMatched = (label, 0)
        else:
            mapLabelMatched = process.extractOne(precinct, mapPrecinctsLabels)
            precinctID = mapPrecincts[mapPrecincts['matchLabel']
                                      == mapLabelMatched[0]]['precinct'].tolist()[0]

        line = "{county},{precinctID},\"{mapLabelMatched}\",\"{precinct}\",{score:.2f}".format(
            county=county, precinctID=precinctID, precinct=precinct, mapLabelMatched=mapLabelMatched[0], score=mapLabelMatched[1])
        f.write(line+"\n")
        if mapLabelMatched[1] < 95:
            print(line)

f.close()
