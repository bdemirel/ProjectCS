#!/bin/bash
#
# clear.sh 'YYYYmmdd' 'dataset'
# For ex;
# routine.sh '20180101' 'alexa1m'
#
# Therefore;
# $1 = 'YYYYmmdd'
# $2 = 'dateset'

#Compress results
tar -cvzf "/data/$USER/results/$2/$1.tar.gz" "/data/$USER/results/$2/$1"

#remove result folder if exists
if [ -d "/data/$USER/results/$2/$1" ]
then
	rm -r "/data/$USER/results/$2/$1"
fi

#remove old data folder if exists
if [ -d "/data/$USER/$2/$1" ]
then
	rm -r "/data/$USER/$2/$1"
fi