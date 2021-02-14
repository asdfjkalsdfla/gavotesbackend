#!/bin/bash
set -e
set -x

ELECTION_NUMBER="$1"
ABSENTEE_DATA_DIR="$2"

curl -sS -D - https://elections.sos.ga.gov/Elections/downLoadVPHFile.do --data-raw "cdElecCat=&idElection=$ELECTION_NUMBER&cdFileType=AB&cdStaticFile=" || true

DLFile="$ABSENTEE_DATA_DIR/$ELECTION_NUMBER-`date +%Y%m%d`.zip"
curl 'https://elections.sos.ga.gov/Elections/downLoadVPHFile.do' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' --compressed -H 'Content-Type: application/x-www-form-urlencoded' -H 'Origin: https://elections.sos.ga.gov' -H 'DNT: 1' -H 'Connection: keep-alive' -H 'Referer: https://elections.sos.ga.gov/Elections/voterabsenteefile.do' -H 'Upgrade-Insecure-Requests: 1' -H 'TE: Trailers' --data-raw "cdElecCat=&idElection=$ELECTION_NUMBER&cdFileType=AB&cdStaticFile=" -o $DLFile
unzip -o $DLFile -d $ABSENTEE_DATA_DIR/data
if [ -f $ABSENTEE_DATA_DIR/data/STATEWIDE.csv ] ; then
    rm $ABSENTEE_DATA_DIR/data/STATEWIDE.csv
fi