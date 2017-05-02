# 20170420 - hvivani
#
#
#!/bin/bash

echo "Running TestDFSIO clean"
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-jobclient-2.7.3-amzn-2-tests.jar TestDFSIO -clean

#-nfiles 1000 -fileSize 1000: 1000 files of 1GB
echo "Running TestDFSIO Write"
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-jobclient-2.7.3-amzn-2-tests.jar TestDFSIO -D mapred.reduce.tasks=1000 -write -nrFiles 5000 -fileSize 2000

echo "Running TestDFSIO Read"
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-jobclient-2.7.3-amzn-2-tests.jar TestDFSIO -D mapred.reduce.tasks=1000 -read -nrFiles 5000 -fileSize 2000
