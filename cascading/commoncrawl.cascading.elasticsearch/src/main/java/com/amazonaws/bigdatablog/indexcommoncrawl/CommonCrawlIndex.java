package com.amazonaws.bigdatablog.indexcommoncrawl;

import cascading.flow.FlowDef;
import cascading.operation.regex.RegexGenerator;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
//import cascading.scheme.local.TextLine;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import org.elasticsearch.hadoop.cascading.EsTap;
import java.util.Properties;

public class CommonCrawlIndex {

    public static FlowDef buildFlowDef(Properties properties){
        // create the Cascading "source" (input) tap to read the commonCrawl WAT file(s)
        System.out.println("platform: " + properties.getProperty("platform"));
        Tap<?, ?, ?>  source=null;
        if (properties.getProperty("platform").toString().compareTo("DISTRIBUTED")==0){
                source = new Hfs(new cascading.scheme.hadoop.TextLine(new Fields("line")),properties.getProperty("inPath"));
        }else if (properties.getProperty("platform").toString().compareTo("LOCAL")==0){
                source = new FileTap(new cascading.scheme.local.TextLine(new Fields("line")) ,properties.getProperty("inPath"));
        }else {
                throw new RuntimeException("Unknown platform: " + properties.getProperty("PLATFORM"));
        }
        //Tap<?, ?, ?> source = new FileTap(new TextLine(new Fields("line")) ,properties.getProperty("inPath"));
        //Tap<?, ?, ?>  source = new Hfs(new TextLine(new Fields("line")),properties.getProperty("inPath"));

        // create the "sink" (output) tap that will export the data to Elasticsearch
        Tap sink = new EsTap(properties.getProperty("es.target.index"));

        //Build the Cascading Flow Definition
        return CommonCrawlIndex.createCommonCrawlFlowDef(source, sink);
    }

    private static FlowDef createCommonCrawlFlowDef(Tap<?, ?, ?> source, Tap<?, ?, ?> sink) {
        Pipe parsePipe = new Pipe( "exportCommonCrawlWATPipe" );

        //Add a Regular Expression to collect the envelope json field from each line in the file
        RegexGenerator splitter=new RegexGenerator(new Fields("json"),"^\\{\"Envelope\".*$");
        parsePipe = new Each( parsePipe, new Fields( "line" ), splitter, Fields.RESULTS );

        // connect the taps, pipes, etc., into a flow
        return FlowDef.flowDef()
                .addSource( parsePipe, source )
                .addTailSink( parsePipe, sink );
    }



}

