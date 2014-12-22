Steps to run the httpd log parser with maven - on EMR cluster

1) On a EMR Cluster, install Maven at the master node. http://hvivani.com.ar/2014/12/16/instalando-maven-en-instancia-amazon-ec2/

2) Create a mvn project: mvn archetype:generate -DgroupId=com.amazonaws.vivanih.hadoop.cascading -DartifactId=loganalysis -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false

3) create ~/loganalysis/pom.xml with this content: https://raw.githubusercontent.com/hvivani/bigdata/master/impatient/part1/pom.xml

4) create Main.java in ~/loganalysis/src/main/java/com/amazonaws/vivanih/hadoop/cascading/ with this content: https://raw.githubusercontent.com/hvivani/bigdata/master/cascading/logparser/Main.java

5) Create the file ~/loganalysis/src/assembly/job.xml with this content: https://raw.githubusercontent.com/hvivani/bigdata/master/impatient/part1/job.xml

6) Create the JARs: mvn clean && mvn assembly:assembly

7) Run cascading logparser application: hadoop jar ~/loganalysis/target/impatient-1.0-SNAPSHOT-job.jar s3://your-bucket/input/httpdlogs/ s3://your-bucket/output/logparser/201412201216/
