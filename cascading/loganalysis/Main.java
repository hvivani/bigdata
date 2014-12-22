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
        
        // sources and sinks
        Tap inTap 	= new Hfs(new TextLine(), inputPath); //input
        Tap outTap  = new Hfs(new TextDelimited(true, ";"), outputPath, SinkMode.REPLACE); //output
        
        // Parse the line of input and break them into five fields
        RegexParser parser = new RegexParser(new Fields("ip", "time", "request", "response", "size"), 
        		"^([^ ]*) \\S+ \\S+ \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) ([^ ]*).*$", new int[]{1, 2, 3, 4, 5});
        
        // This pipe will process each line
        Pipe processPipe = new Each("processPipe", new Fields("line"), parser, Fields.RESULTS);
        
        // Grouping by 'ip' field
        processPipe = new GroupBy(processPipe, new Fields("ip"));
        
        // Aggregate each "ip" group using Count function
        processPipe = new Every(processPipe, Fields.GROUP, new Count(new Fields("IPcount")), Fields.ALL);
        
        // After aggregation counter for each "ip," sort the counts
        //Pipe sortedCountByIpPipe = new GroupBy(processPipe, new Fields("IPcount"), true);
        
        // Limit them to the first 10, in the descending order
        //sortedCountByIpPipe = new Each(sortedCountByIpPipe, new Fields("IPcount"), new Limit(10));
        
        // Join the pipe together in the flow, creating inputs and outputs (taps)
        FlowDef flowDef = FlowDef.flowDef()
    		   .addSource(processPipe, inTap)
    		   //.addTailSink(sortedCountByIpPipe, outTap)
    		   .addTailSink(processPipe, outTap)
    		   .setName("DataProcessing");
        Properties properties = AppProps.appProps()
        		.setName("DataProcessing")
        		.buildProperties();
        Flow parsedLogFlow = new HadoopFlowConnector(properties).connect(flowDef);
        
        //Finally, execute the flow.
        parsedLogFlow.complete();
    }
}
