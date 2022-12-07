#!/usr/bin/env bash
set -e
set -x

echo "County Number,Registration Number,Election Date,Election Type,Party,Absentee,Provisional,Supplemental"
# awk -v OFS=, '{ print substr($0, 1, 3), substr($0, 4, 8), substr($0, 12, 8), substr($0, 20, 3), substr($0, 23, 2), substr($0, 25, 1), substr($0, 26, 1), substr($0, 27, 1)}' $1
awk -v FIELDWIDTHS="3 8 8 3 2 1 1 1" -v OFS=, '{gsub(/ /, "", $5); print $1,$2,$3,$4,$5,$6,$7,$8}'  $1