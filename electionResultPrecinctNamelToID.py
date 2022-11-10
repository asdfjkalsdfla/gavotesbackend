#!/usr/bin/env python3
import pandas as pd
from thefuzz import process
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("year")
args = parser.parse_args()
year = args.year

electionResults = pd.read_csv('data/electionResults/'+year+'/'+year+'_general.csv')
electionResultsCountiesAndPrecincts = electionResults[['county','precinct']].drop_duplicates()
electionResultsCountiesAndPrecincts['county'] = electionResults['county'].str.upper()
electionResultsCounties = electionResultsCountiesAndPrecincts['county'].unique().tolist()


mapCountyPrecinctList = pd.read_csv('data/geojson/precincts/2020_simple/GA_precincts_id_to_name.csv')
mapCountyPrecinctList['matchLabel'] = mapCountyPrecinctList['precinctName']

f = open('data/electionResults/'+year+'/'+year+'_general_map_ids.csv', "w")
f.write('county,precinct,mapPrecinctName,electionResultsPrecinctName,score\n')

for county in electionResultsCounties :
    mapPrecincts = mapCountyPrecinctList[mapCountyPrecinctList['county']==county]
    mapPrecinctsLabels = mapPrecincts['matchLabel'].tolist()
    precinctsInCounty = electionResultsCountiesAndPrecincts[electionResultsCountiesAndPrecincts['county']==county]['precinct'].tolist()
    for precinct in precinctsInCounty :
        if county == "CHATHAM" or county == "WARE" or county == "FORSYTH" :
            if (year == "2018" or year == "2016"):
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
        elif county == "RICHMOND" and year == "2022":
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
        
        line  = "{county},{precinctID},{mapLabelMatched},{precinct},{score:.2f}".format(county=county,precinctID=precinctID,precinct=precinct,mapLabelMatched=mapLabelMatched[0],score=mapLabelMatched[1])
        if mapLabelMatched[1] > 50 or len(precinctsInCounty)==0 :
            f.write(line+"\n")
        if mapLabelMatched[1] < 95 :
            print(line)

f.close()