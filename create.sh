#!/bin/bash
#
# create.sh 'YYYY' 'dd Mon' 'dataset'
# For ex;
# create.sh '2018' '01 Jan' 'alexa1m'
#
# Therefore;
# $1 = 'YYYY'
# $2 = 'dd Mon'
# $3 = 'dateset'

#Date to be downloaded, in "dd Mon YYYY" format
downl_date="$2 $1"

#Date format to use. OpenINTEL uses YYYYMMDD
format="%Y%m%d"

#OpenIntel date
pulldate=$(date -d "$downl_date" +"$format")

#wget command to download archive into /data
wget -P "/data/$USER" "https://data.openintel.nl/data/$3/$1/openintel-$3-$pulldate.tar"

#create extraction folder
if [ ! -d "/data/$USER/$3/$pulldate" ]
then
	mkdir -p "/data/$USER/$3/$pulldate"
fi

#extract the tar file into the created folder
tar -C "/data/$USER/$3/$pulldate" -xvf "/data/$USER/openintel-$3-$pulldate.tar"

#remove archive
rm "/data/$USER/openintel-$3-$pulldate.tar"

#create result folder
if [ ! -d "/data/$USER/results/$3/$pulldate" ]
then
	mkdir -p "/data/$USER/results/$3/$pulldate"
fi