#!/usr/bin/env bash

# checks pgyi dataset for fkey integrity
# will print out any offending records
# usage: bash ./fkeycheck.sh <csv_data_folder>
# example: bash ./fkeycheck.sh ./examples/pgyi/data
# example output: plot_measurement.csv,n/a,foreign-key,missing referenced parent plot.csv: GOA109,error,2

wd=$1

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
    awk -F, " \
        NR == FNR { p=$2; k[$2]=1; next; } \
        NF { if(!k[$4]) vs[$4] ? vs[$4]++ : vs[$4]=1; } \
        END { for (v in vs) print NR FS \"$3\" FS p FS \"foreign-key\" FS \"error\" FS \"missing parent record in $1: \" v}" $wd/$1 $wd/$3
}

echo source_row_index,violating_table,violation_key,violation_type,violation_severity,violation_comment

for child in "${plot_childs[@]}"
do
    check $plot '$1 "-" $2' $child '$1 "-" $2'
done

check $trees '$1 "-" $2 "-" $3' $trees_measurement '$1 "-" $2 "-" $4'
check $plot_measurement '$1 "-" $2 "-" $3' $trees_measurement '$1 "-" $2 "-" $3'
check $plot_measurement '$1 "-" $2 "-" $3' $regeneration '$1 "-" $2 "-" $3'
