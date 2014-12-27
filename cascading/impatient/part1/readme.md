Steps to run cascading impatient part 1 - Maven Build - On EMR cluster

Source Reference:
http://docs.cascading.org/impatient/impatient1.html


1) On a EMR Cluster, install Maven at the master node.
http://hvivani.com.ar/2014/12/16/instalando-maven-en-instancia-amazon-ec2/

2) Create a mvn project:
mvn archetype:generate -DgroupId=com.amazonaws.vivanih.hadoop.cascading -DartifactId=impatient -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false

3) create ~/impatient/pom.xml with this content:
https://raw.githubusercontent.com/hvivani/bigdata/master/cascading/impatient/part1/pom.xml

4) create Main.java in ~/impatient/src/main/java/com/amazonaws/vivanih/hadoop/cascading/ with this content:
https://raw.githubusercontent.com/hvivani/bigdata/master/cascading/impatient/part1/Main.java

5) Create the file ~/impatient/src/assembly/job.xml with this content:
https://raw.githubusercontent.com/hvivani/bigdata/master/cascacind/impatient/part1/job.xml

6) Create the JARs:
mvn clean && mvn assembly:assembly

7) Run cascading impatient part 1 application:
hadoop jar ~/impatient/target/impatient-1.0-SNAPSHOT-job.jar s3://your-bucket/input/rain.txt s3://your-bucket/output/impatient/part1/
