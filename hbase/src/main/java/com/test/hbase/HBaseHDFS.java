//This example uses HBase as a MapReduce source and sink to HDFS. This example will count the number of distinct instances of a value in a table and write those summarized counts to HDFS. 


package com.test.hbase;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.io.*;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;


public class HBaseHDFS {


// In this example mapper a column with a String-value is chosen as the value to summarize upon. This value is used as the key to emit from the mapper, and an IntWritable represents an instance counter. 
public static class MyHBaseMapper extends TableMapper<Text, IntWritable>  {

	private final IntWritable ONE = new IntWritable(1);
   	private Text text = new Text();

   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
			        	String val = new String(value.getValue(Bytes.toBytes("cf"), Bytes.toBytes("a")));
          	text.set(val);     // we can only emit Writables...
        	context.write(text, ONE);
   	}
}

// This is a "generic" Reducer instead of extending TableMapper and emitting Puts.
public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>  {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int i = 0;
		for (IntWritable val : values) {
			i += val.get();
		}
		context.write(key, new IntWritable(i));
	}
}
						  		 									   	    
					    		   	  	  		 									   	    
    public static void main(String[] args) throws Exception {
       Configuration conf = new Configuration();
       // HBase through MR on Yarn is trying to connect to localhost instead of quorum.
       conf.set("hbase.zookeeper.quorum","172.31.3.246");
       conf.set("hbase.zookeeper.property.clientPort","2181");

       Job job = new Job(conf, "hbase");
       job.setJarByClass(HBaseHDFS.class);

       Scan scan = new Scan();
       scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
       scan.setCacheBlocks(false);  // don't set to true for MR jobs

       TableMapReduceUtil.initTableMapperJob(
	  "test1",        // input HBase table name
          scan,             // Scan instance to control CF and attribute selection
	  MyHBaseMapper.class,   // mapper
          Text.class,             // mapper output key
	  IntWritable.class,             // mapper output value
       job);

      /* TableMapReduceUtil.initTableReducerJob(
	"OutPutTable",      // output table
	null,             // reducer class
       job);*/

       job.setReducerClass(MyReducer.class);    // reducer class
       job.setNumReduceTasks(1);    // at least one, adjust as required
       //FileOutputFormat.setOutputPath(job, new Path("/tmp/mr/mySummaryFile32"));  // adjust directories as required
       FileOutputFormat.setOutputPath(job, new Path(args[0]));  // adjust directories as required
       
       //job.setNumReduceTasks(0);
       //job.setOutputFormatClass(NullOutputFormat.class);   // because we aren't emitting anything from mapper

       boolean b = job.waitForCompletion(true);
       if (!b) {
	  throw new IOException("error with job!");
       }

    }
}

