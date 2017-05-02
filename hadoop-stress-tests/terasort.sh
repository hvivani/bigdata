# 20170420 - hvivani
#
#!/bin/bash

runs=$1

if [ -z $runs ]; then
    runs=50
fi

echo "remove input/output directory if any"
hadoop fs -rmr /terasort-output
hadoop fs -rmr /terasort-input

echo "running teragen output to /terasort-input"
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar teragen 100000000000 /terasort-input >> /mnt/teragen.out

echo "entering for terasort"
count=0
while [ $count -lt $runs ]; do
    echo "we are starting terasort run " $count " of " $runs "..."
    hadoop jar /usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar terasort /terasort-input /terasort-output/$count >> /mnt/terasort.out
    let count=count+1
done
