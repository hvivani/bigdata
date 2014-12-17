
1) On a EMR Cluster, install Maven at the master node.
http://hvivani.com.ar/2014/12/16/instalando-maven-en-instancia-amazon-ec2/

2) Create a mvn project:
mvn archetype:generate -DgroupId=com.amazonaws.vivanih.hadoop.cascading -DartifactId=impatient -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false

3) create ~/impatient/pom.xml with this content:

4) create Main.java in ~/impatient/src/main/java/com/amazonaws/dags/hadoop/cascading/ with this content:

5) Create the file ~/impatient/src/assembly/job.xml with this content:

6) Create the JARs:
mvn clean && mvn assembly:assembly

7) Run cascading impatient part 1 application:
hadoop jar impatient-1.0-SNAPSHOT-job.jar s3://your-bucket/input/rain.txt s3://your-bucket/output/impatient/
