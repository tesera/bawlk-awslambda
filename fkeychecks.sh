#!/usr/bin/env bash

# checks pgyi dataset for fkey integrity
# will print out any offending rows as violation records
# usage: bash ./fkeycheck.sh <csv_data_folder>
# example: bash ./fkeycheck.sh ./examples/pgyi/data
# example output: 2,disturbance.csv,COM-123,foreign-key,error,missing parent record in plot.csv

wd="$1"

plot=plot.csv
plot_measurement=plot_measurement.csv
trees=trees.csv
trees_measurement=trees_measurement.csv
treatment=treatment.csv
disturbance=disturbance.csv
regeneration=regeneration.csv
photo_avi=photo_avi.csv

plot_childs=($plot_measurement $trees $treatment $disturbance $regeneration $photo_avi)

check(){
    # 1. build array of all parent pkeys
    # 2. iterate each child and if fkey not in pkeys add to orphans
    # 3. print each orphan as violation record

    awk -F, " \
        NR == FNR { pkey=$2; pkeys[pkey]=1; next; } \
        NF { fkey=$4; if(!pkeys[fkey]) orphans[fkey]=FNR } \
        END { for (orphan in orphans) print orphans[orphan] FS \"$3\" FS orphan FS \"foreign-key\" FS \"error\" FS \"missing parent record in $1: \" orphan}" "$wd/$1" "$wd/$3" | \
        sed "s/\"/'/g"
}

# echo violation csv record headers
echo source_row_index,violating_table,violation_key,violation_type,violation_severity,violation_comment

# check all direct childs of plots
for child in "${plot_childs[@]}"
do
    check $plot '$1 "-" $2' $child '$1 "-" $2'
done

# check all other relationships
check $trees '$1 "-" $2 "-" $3' $trees_measurement '$1 "-" $2 "-" $4'
check $plot_measurement '$1 "-" $2 "-" $3' $trees_measurement '$1 "-" $2 "-" $3'
check $plot_measurement '$1 "-" $2 "-" $3' $regeneration '$1 "-" $2 "-" $3'
