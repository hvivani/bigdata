HBase mapreduce example, copy records from one table to the other.

Create a new maven project:
mvn archetype:generate -DarchetypeGroupId=org.apache.maven.archetypes -DgroupId=com.test.hbase -DartifactId=hbase

Set cluster config:
export HADOOP_CLASSPATH="/usr/lib/hbase/*:/usr/lib/hbase/lib/*"

Compile:
mvn clean install

Run:
hadoop jar target/hbase-0.0.1-SNAPSHOT.jar com.test.hbase.HBase

References:
http://hbase.apache.org/0.94/book/mapreduce.example.html
http://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hbase-s3.html
https://hvivani.com.ar/2015/02/28/hbase-useful-commands/

