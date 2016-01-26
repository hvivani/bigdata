#!/bin/bash
#hvivani. 20160126
#Elasticsearch Dummy Indexer: Indexes dummy documents into elasticsearch using REST API

endpoint="localhost:9200"

while :
do
        random_id=$RANDOM
        random_text_1=`/usr/bin/tr -dc A-Za-z0-9 </dev/urandom | head -c 1000`
        random_text_2=`/usr/bin/tr -dc A-Za-z0-9 </dev/urandom | head -c 1000`
        echo "Press [CTRL+C] to stop.."
        curl -XPUT "$endpoint/dummy/data/$random_id" -d' { "id": '$random_id', "text_field_1": "'$random_text_1'", "text_field_2": "'$random_text_2'"}'
        #sleep 1
done
