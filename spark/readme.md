Create package:
mvn archetype:generate -DgroupId=org.apache.spark.examples -DartifactId=sparkPi -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false

Compile/Build:
 mvn clean && mvn package -e

Run (Hadoop 2.4 cluster):
/home/hadoop/spark/bin/spark-submit --deploy-mode cluster --master yarn-cluster --class org.apache.spark.examples.JavaSparkPi ~/sparkPi/target/sparkPi-1.0-SNAPSHOT.jar 10

Debug:
 yarn logs --applicationId application_1423287226115_003

References:
http://spark.apache.org/docs/1.2.0/running-on-yarn.html
