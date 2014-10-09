Quickstart Guide
================

Testing the scripts:

Testing mapper:

$ echo "foo foo quux labs foo bar quux" | ./mapper-wc.py
foo     1
foo     1
quux    1
labs    1
foo     1
bar     1
quux    1


Testing mapper and reducer:

$ echo "foo foo quux labs foo bar quux" | ./mapper-wc.py | sort -k1,1 | ./reducer-wc.py
bar     1
foo     3
labs    1
quux    2

Launching a cluster using EMR CLI tools:

./elastic-mapreduce --create --name WordCount-Py --alive --key-pair vivanih-public-key --enable-debugging --stream \
--ami-version 3.1.0 \
--num-instances 3 \
--instance-type m1.medium \
--step-name WordCount \
--step-action CONTINUE \
--input s3://vivanih-emr/input/test-data.txt \
--arg "files" --arg "s3://vivanih-emr/scripts/mapper-wc.py,s3://vivanih-emr/scripts/reducer-wc.py" \
--mapper s3://vivanih-emr/scripts/mapper-wc.py \
--reducer s3://vivanih-emr/scripts/reducer-wc.py \
--output s3://vivanih-emr/WordCount-Py/Output2014082010125
