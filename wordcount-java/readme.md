Download hadoop-core from Maven repository to be able to compile sample code:

$ wget http://repo1.maven.org/maven2/org/apache/hadoop/hadoop-core/0.20.2/hadoop-core-0.20.2.jar
$ mkdir wordcount_classes

Compiling and creating jar:

$ javac -classpath hadoop-core-0.20.2.jar -d wordcount_classes WordCount.java
$ jar -cvf wordcount.jar -C wordcount_classes/ .

Execute the MapReduce Job

To run the job, we’ll run the Hadoop script and pass in a reference to the JAR file, the name of the class containing the main method, and an input file and an output file:

$ hadoop  jar wordcount.jar org.myorg.WordCount /user/0001 /user/RESULTS
