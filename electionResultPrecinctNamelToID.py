#!/usr/bin/env python3
import pandas as pd
from thefuzz import process
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("race")
parser.add_argument("--county", help="specifies the county")
args = parser.parse_args()
race = args.race

electionResults = pd.read_csv('data/electionResults/'+race+'/'+race+'.csv')
electionResultsCountiesAndPrecincts = electionResults[['county','precinct']].drop_duplicates()
electionResultsCountiesAndPrecincts['county'] = electionResults['county'].str.upper()
if args.county:
    electionResultsCountiesAndPrecincts = electionResultsCountiesAndPrecincts[electionResultsCountiesAndPrecincts['county'] == args.county.upper()]
electionResultsCounties = electionResultsCountiesAndPrecincts['county'].unique().tolist()


mapCountyPrecinctList = pd.read_csv('data/geojson/precincts/2020_simple/GA_precincts_id_to_name.csv')
mapCountyPrecinctList['matchLabel'] = mapCountyPrecinctList['precinctName']

manualMapLabelToOverrides = pd.read_csv('data/electionResults/'+race+'/'+race+'_map_ids_manual.csv', dtype={'precinct':'string'})

f = open('data/electionResults/'+race+'/'+race+'_map_ids.csv', "w")
f.write('county,precinct,mapPrecinctName,electionResultsPrecinctName,score\n')

for county in electionResultsCounties :
    mapPrecincts = mapCountyPrecinctList[mapCountyPrecinctList['county']==county]
    mapPrecinctsLabels = mapPrecincts['matchLabel'].tolist()
    precinctsInCounty = electionResultsCountiesAndPrecincts[electionResultsCountiesAndPrecincts['county']==county]['precinct'].tolist()
    # print(precinctsInCounty)
    mapPrecinctsInCountyID = mapCountyPrecinctList[mapCountyPrecinctList['county']==county]['precinct'].tolist()
    # print(mapPrecinctsInCountyID)
    # print(mapPrecinctsLabels)
    
    for precinct in precinctsInCounty :
        overrides = manualMapLabelToOverrides[( (manualMapLabelToOverrides['county']==county) & (manualMapLabelToOverrides['electionResultsPrecinctName']==precinct))]
        if(overrides.size > 0) : 
            # precinctID = overrides.at[0, 'precinct']
            precinctID = overrides.iat[0, 1]
            label = overrides.iat[0, 2]
            mapLabelMatched=(label,100)
        elif county == "CHATHAM" or county == "WARE" or county == "FORSYTH" :
            if (race == "2018" or race == "2016"):
                precinctID = precinct.split(" ")[0]+"C"
            else:
                precinctID = precinct.split(" ")[0]
            labelList = mapPrecincts[mapPrecincts['precinct']==precinctID ]['precinctName'].tolist()
            if len(labelList) > 0 :
                label=labelList[0]
                apLabelMatched=(label,100)
            else :
                label="??"
                mapLabelMatched=(label,0)
        elif county == "RICHMOND" and race == "2022":
            precinctID = precinct
            labelList = mapPrecincts[mapPrecincts['precinct']==precinctID ]['precinctName'].tolist()
            if len(labelList) > 0 :
                label=labelList[0]
                mapLabelMatched=(label,100)
            else :
                label="??"
                mapLabelMatched=(label,0)
        else :
            mapLabelMatched = process.extractOne(precinct, mapPrecinctsLabels)
            precinctID = mapPrecincts[mapPrecincts['matchLabel']==mapLabelMatched[0] ]['precinct'].tolist()[0]
        
        line  = "{county},{precinctID},\"{mapLabelMatched}\",\"{precinct}\",{score:.2f}".format(county=county,precinctID=precinctID,precinct=precinct,mapLabelMatched=mapLabelMatched[0],score=mapLabelMatched[1])
        f.write(line+"\n")
        if mapLabelMatched[1] < 95 :
            print(line)

f.close()