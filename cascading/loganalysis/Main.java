// This program aggregates by IP hits on apache access logs. 
// It can get the top ten of IP's
//

package com.amazonaws.vivanih.hadoop.cascading;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.aggregator.Count;
import cascading.operation.filter.Sample;
import cascading.operation.filter.Limit;
import cascading.operation.regex.RegexParser;
import cascading.operation.text.DateParser;
import cascading.pipe.*;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import java.util.Properties;
 
public class Main {
    public static void main(String[] args) {
 
        String inputPath 		= args[0]; //input will use default filesystem. HDFS or S3.
        String outputPath 	= args[1]; 
        String sortedTopTen = args[ 1 ] + "/top10/";
        
        
        // sources and sinks
        Tap inTap 	= new Hfs(new TextLine(), inputPath); //input
        Tap outTap  = new Hfs(new TextDelimited(true, ";"), outputPath, SinkMode.REPLACE); //output
        Tap top10Tap = new Hfs( new TextDelimited(true, ";"), sortedTopTen, SinkMode.REPLACE);
        
        // Parse the line of input and break them into five fields
        RegexParser parser = new RegexParser(new Fields("ip", "time", "request", "response", "size"), 
        		"^([^ ]*) \\S+ \\S+ \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) ([^ ]*).*$", new int[]{1, 2, 3, 4, 5});
        
        // "Each" pipe applies a Function or Filter Operation to each Tuple that passes through it.
        Pipe top10Pipe = new Each("top10Pipe", new Fields("line"), parser, Fields.RESULTS);
        Pipe totalPipe = new Each("totalPipe", new Fields("line"), parser, Fields.RESULTS);
        
        // Grouping by 'ip' field: 
        // "GroupBy" manages one input Tuple stream and, groups the stream on selected fields in the tuple stream. 
        top10Pipe = new GroupBy(top10Pipe, new Fields("ip"));
        totalPipe = new GroupBy(totalPipe, new Fields("ip"));
        
        // Aggregate each "ip" group using Count function:
        // "Every" pipe applies an Aggregator (like count, or sum) or Buffer (a sliding window) Operation to every group of Tuples that pass through it.
        top10Pipe = new Every(top10Pipe, Fields.GROUP, new Count(new Fields("IPcount")), Fields.ALL);
        totalPipe = new Every(totalPipe, Fields.GROUP, new Count(new Fields("IPcount")), Fields.ALL);
        
        // After aggregation counter for each "ip," sort the counts. "true" is descending order
        Pipe top10CountPipe = new GroupBy(top10Pipe, new Fields("IPcount"), true);
        
        // Limit them to the first 10, in the descending order
        top10CountPipe = new Each(top10CountPipe, new Fields("IPcount"), new Limit(10));
        
        // Join the pipe together in the flow, creating inputs and outputs (taps)
        FlowDef flowDef = FlowDef.flowDef()
    		   .addSource(top10Pipe, inTap)
    		   //.addTailSink(top10Pipe, outTap) // comment to use sorted top 10
    		   .addTailSink(totalPipe, outTap) //uncomment to use sorted top 10
    		   .addTailSink(top10CountPipe, top10Tap) //uncomment to use sorted top 10
    		   .setName("Top10IP");
        Properties properties = AppProps.appProps()
        		.setName("Top10IP")
        		.buildProperties();
        Flow parsedLogFlow = new HadoopFlowConnector(properties).connect(flowDef);
        
        //Finally, execute the flow.
        parsedLogFlow.complete();
    }
}
