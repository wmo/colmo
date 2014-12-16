#!/bin/bash 

if [ -z $1 ]
then
    echo "Argument needed: number of morsels to launch!" 
else
    seq 1 $1 | while read N; do
        M="mrs-$N" 
        if [ -d $M ] 
        then
            echo "Morsel dir $M already exists, cleaning up"  
            rm -rf $M/*
        else
            echo "Creating morsel dir $M"  
            mkdir $M 
        fi 
        # launch background process, that will use that scratch dir
        ./morsel.py $M  & 
        # sleep 2 seconds, so that not all morsels kick-in at the same time
        sleep 2
    done
fi
echo "remember:" 
echo "#  killall morsel.py " 
echo "#  rm -rf mrs-* " 
