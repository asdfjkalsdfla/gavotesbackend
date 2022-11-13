echo "County Number,Registration Number,Election Date,Election Type,Party,Absentee,Provisional,Supplemental"
awk -v OFS=, '{ print substr($0, 1, 3), substr($0, 4, 8), substr($0, 12, 8), substr($0, 20, 3), substr($0, 23, 2), substr($0, 25, 1), substr($0, 26, 1), substr($0, 27, 1)}' $1
