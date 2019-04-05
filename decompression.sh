#!/bin/bash

count=$(ls -R -l /data/$USER/results/$2/parse | grep .*.json | wc -l)

if [ $count -lt 12 ]
then
	file=$(ls /data/$USER/results/$2 | grep $1.*.tar.gz | head -1)
	
	if [ -f "/data/$USER/results/$2/$file" ]
	then
		tar -C "/data/$USER/results/$2/parse" -xvzf "/data/$USER/results/$2/$file" --xform='s#^.+/##x'
		mv "/data/$USER/results/$2/$file" "/data/$USER/results/$2/decomp"
	fi
fi