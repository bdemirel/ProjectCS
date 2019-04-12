#!/bin/bash

file=$(ls /data/$USER/results/$2 | grep $1.*.tar.gz | head -1)

if [ -f "/data/$USER/results/$2/$file" ]
then
	tar -C "/data/$USER/results/$2/parse" -xvf "/data/$USER/results/$2/$file" --xform='s#^.+/##x'
	echo $file
	mv "/data/$USER/results/$2/$file" "/data/$USER/results/$2/decomp"
fi