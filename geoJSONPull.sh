#!/usr/bin/env bash
set -e
set -x

GEOJSON_DATA_DIR="data/geojson/"

declare -A shape0=(
    [year]='2018'
    [dlFile]='https://www.legis.ga.gov/api/document/docs/default-source/reapportionment-document-library/vtd2018-shapefile.zip?sfvrsn=d01286c5_2'
    [extractFile]="VTD2018-Shapefile.shp"
)
declare -A shape1=(
    [year]='2016'
    [dlFile]="https://www.legis.ga.gov/api/document/docs/default-source/reapportionment-document-library/vtd2016-shape.zip?sfvrsn=2a170a08_2"
    [extractFile]="VTD2016-Shape.shp"
)
declare -A shape2=(
    [year]='2014'
    [dlFile]="https://www.legis.ga.gov/api/document/docs/default-source/reapportionment-document-library/vtd2014-shape.zip?sfvrsn=c3a31adc_2"
    [extractFile]="VTD2014-SHAPE.shp"
)
declare -A shape3=(
    [year]='2012'
    [dlFile]="https://www.legis.ga.gov/api/document/docs/default-source/reapportionment-document-library/voting-precinct-2012.zip?sfvrsn=952212ae_2"
    [extractFile]="VTD12.shp"
)

declare -n shape

for shape in ${!shape@}; do
    GEOJSON_YEAR="${shape[year]}"
    # echo $GEOJSON_YEAR
    mkdir -p $GEOJSON_DATA_DIR/$GEOJSON_YEAR
    DLFile="$GEOJSON_DATA_DIR/$GEOJSON_YEAR/`date +%Y%m%d`.zip"
    curl "${shape[dlFile]}" -o $DLFile
    unzip -o $DLFile -d $GEOJSON_DATA_DIR/$GEOJSON_YEAR
    yarn shp2json data/geojson/$GEOJSON_YEAR/${shape[extractFile]} -o data/geojson/$GEOJSON_YEAR/GeoJSON$GEOJSON_YEAR.json
done 
