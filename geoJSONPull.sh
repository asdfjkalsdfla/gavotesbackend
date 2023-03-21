#!/usr/bin/env bash
set -e
set -x

# https://www.legis.ga.gov/joint-office/reapportionment

GEOJSON_DATA_DIR="data/geojson/precincts/"

declare -A shape0=(
    [year]='2022'
    [dlFile]='https://www.legis.ga.gov/api/document/docs/default-source/reapportionment-document-library/ga-precincts2022-shape.zip?sfvrsn=7844a3b6_2'
    [extractFile]="GA-PRECINCTS2022-SHAPE.shp"
)

declare -A shape0=(
    [year]='2020'
    [dlFile]='https://www.legis.ga.gov/api/document/docs/default-source/reapportionment-document-library/vtd2020-shape-4-20-21.zip?sfvrsn=93e56ae7_2'
    [extractFile]="VTD2020-Shape.shp"
)

declare -A shape1=(
    [year]='2018'
    [dlFile]='https://www.legis.ga.gov/api/document/docs/default-source/reapportionment-document-library/vtd2018-shapefile.zip?sfvrsn=d01286c5_2'
    [extractFile]="VTD2018-Shapefile.shp"
)
declare -A shape2=(
    [year]='2016'
    [dlFile]="https://www.legis.ga.gov/api/document/docs/default-source/reapportionment-document-library/vtd2016-shape.zip?sfvrsn=2a170a08_2"
    [extractFile]="VTD2016-Shape.shp"
)
declare -A shape3=(
    [year]='2014'
    [dlFile]="https://www.legis.ga.gov/api/document/docs/default-source/reapportionment-document-library/vtd2014-shape.zip?sfvrsn=c3a31adc_2"
    [extractFile]="VTD2014-SHAPE.shp"
)
declare -A shape4=(
    [year]='2012'
    [dlFile]="https://www.legis.ga.gov/api/document/docs/default-source/reapportionment-document-library/voting-precinct-2012.zip?sfvrsn=952212ae_2"
    [extractFile]="VTD12.shp"
)

declare -n shape

for shape in ${!shape@}; do
    GEOJSON_YEAR="${shape[year]}"
    # echo $GEOJSON_YEAR
    mkdir -p "$GEOJSON_DATA_DIR/$GEOJSON_YEAR/"
    mkdir -p "$GEOJSON_DATA_DIR/${GEOJSON_YEAR}_simple/"
    DLFile="$GEOJSON_DATA_DIR/$GEOJSON_YEAR/`date +%Y%m%d`.zip"
    geoJSONFileName="$GEOJSON_DATA_DIR/$GEOJSON_YEAR/GA_precincts_$GEOJSON_YEAR.json"
    geoJSONSimpleFileName="$GEOJSON_DATA_DIR/${GEOJSON_YEAR}_simple/GA_precincts_simple_$GEOJSON_YEAR.json"
    curl "${shape[dlFile]}" -L -o $DLFile
    unzip -o $DLFile -d $GEOJSON_DATA_DIR/$GEOJSON_YEAR
    yarn shp2json $GEOJSON_DATA_DIR/$GEOJSON_YEAR/${shape[extractFile]} -o $geoJSONFileName
    ./geoJSONSimplify.js  -i $geoJSONFileName -o $geoJSONSimpleFileName -p
done

./geoJSONBoundingBox.js -i ~/dev/play/gavote/website/frontend/public/static/GA_counties_simple.json -o ~/dev/play/gavote/website/frontend/src/VotesMapCountesBB.json
