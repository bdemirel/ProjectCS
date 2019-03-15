#!/bin/bash
#
# routine.sh 'YYYY' 'dd Mon' 'dataset'
# For ex;
# routine.sh '2018' '01 Jan' 'alexa1m'
#
# Therefore;
# $1 = 'YYYY'
# $2 = 'dd Mon'
# $3 = 'dateset'

#Date to be downloaded, in "dd Mon YYYY" format
downl_date="$2 $1"

#Sets how many days of data to keep at the same time, in "-N days" format
removal_interval="-3 days"

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
if [ ! -d "/home/$USER/Thesis/results/$3/$pulldate" ]
then
	mkdir -p "/home/$USER/Thesis/results/$3/$pulldate"
fi

#date to remove
removaldate=$(date -d "$downl_date $removal_interval" +"$format")

#remove old folder if exists
if [ -d "/data/$USER/$3/$removaldate" ]
then
	rm -r "/data/$USER/$3/$removaldate"
fi