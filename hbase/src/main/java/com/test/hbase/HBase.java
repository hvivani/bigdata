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


public class HBase {



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
       // HBase through MR on Yarn is trying to connect to localhost instead of quorum.
       conf.set("hbase.zookeeper.quorum","172.31.3.246");
       conf.set("hbase.zookeeper.property.clientPort","2181");

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

    }
}

