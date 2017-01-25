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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.io.*;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.hbase.KeyValue;


public class HBase {

  /*public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
      private final static IntWritable one = new IntWritable(1);
      private Text word = new Text();

      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         String line = value.toString();
         StringTokenizer tokenizer = new StringTokenizer(line);
         while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            context.write(word, one);
         }
      }
   }*/

  /* public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

      public void reduce(Text key, Iterable<IntWritable> values, Context context)
	 throws IOException, InterruptedException {
            int sum = 0;
	    for (IntWritable val : values) {
	       sum += val.get();
            }
	    context.write(key, new IntWritable(sum));
	 }
      }*/

/*   public static class MyHBaseMapper extends TableMapper<Text, Text> {

	     public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
		         // process data for the row from the Result instance.
	     }
   }*/

public static class MyHBaseMapper extends TableMapper<ImmutableBytesWritable, Put>  {
	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
		// this example is just copying the data from the source table...
 		context.write(row, resultToPut(row,value));
 	}
			
        private static Put resultToPut(ImmutableBytesWritable key, Result result) throws IOException {
 	 	Put put = new Put(key.get());
		for (KeyValue kv : result.raw()) {
		put.add(kv);
		}
	 return put;
	}
}
						  		 									   	    
					    		   	  	  		 									   	    
    public static void main(String[] args) throws Exception {
       Configuration conf = new Configuration();
       Job job = new Job(conf, "hbase");
       job.setJarByClass(HBase.class);

       Scan scan = new Scan();
       scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
       scan.setCacheBlocks(false);  // don't set to true for MR jobs

       TableMapReduceUtil.initTableMapperJob(
	  "test",        // input HBase table name
          scan,             // Scan instance to control CF and attribute selection
	  MyHBaseMapper.class,   // mapper
          null,             // mapper output key
	  null,             // mapper output value
       job);

       TableMapReduceUtil.initTableReducerJob(
	"OutPutTable",      // output table
	null,             // reducer class
       job);
       
       job.setNumReduceTasks(0);
       //job.setOutputFormatClass(NullOutputFormat.class);   // because we aren't emitting anything from mapper

       boolean b = job.waitForCompletion(true);
       if (!b) {
	  throw new IOException("error with job!");
       }

      // job.setOutputKeyClass(Text.class);
      // job.setOutputValueClass(IntWritable.class);

      // job.setMapperClass(Map.class);
      // job.setReducerClass(Reduce.class);

      // job.setInputFormatClass(TextInputFormat.class);
      // job.setOutputFormatClass(TextOutputFormat.class);

      // FileInputFormat.addInputPath(job, new Path(args[0]));
      // FileOutputFormat.setOutputPath(job, new Path(args[1]));

       //job.waitForCompletion(true);
    }
}

