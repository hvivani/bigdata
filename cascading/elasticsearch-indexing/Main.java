package com.amazonaws.vivanih.hadoop.cascading;

import java.util.Properties;

import org.elasticsearch.hadoop.cascading.EsTap;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.Average;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.operation.regex.RegexGenerator;
import cascading.operation.regex.RegexFilter;
import cascading.operation.text.DateParser;
import cascading.operation.text.DateFormatter;
import cascading.operation.text.FieldFormatter;
import cascading.tap.SinkMode;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Unique;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;



public class
  Main
  {
  public static void
  main( String[] args )
    {
    String inPath = args[ 0 ];

    Properties properties = new Properties();
    AppProps.setApplicationJarClass( properties, Main.class );
    //we tell cascading we are using JSON files as input:
    //As this is just used when calling EsTap library, we can apply some filters before output is indexed on Elasticsearch
    properties.setProperty("es.input.json", "true");
    //disable speculative execution
    properties.setProperty("mapreduce.map.speculative", "false");
    properties.setProperty("mapreduce.reduce.speculative", "false");
    //elasticsearch bootstrapaction uses port 9202 on the slave nodes. As 9200 is used for HDFS internode communication.
    properties.setProperty("es.port", "9202");

    //by default index will be auto created
    //propperties.setProperty("es.index.auto.create", "false");

    FlowConnector flowConnector = new HadoopFlowConnector( properties );

    // create the source tap for JSON input
    Tap inTap = new Hfs( new TextLine(new Fields("line")), inPath );

    // create the sink tap
    // Output is sent directly to Elasticsearch
    Tap outTap = new EsTap("common-crawl/wat");
    //if we want to write to HDFS instead of elasticsearch:
    //Tap outTap = new Hfs( new TextLine(new Fields("line")), outPath );

    //The input files are parsed using Regex to remove WARC Headers and process a clean JSON file as input
    Pipe parsePipe = new Pipe( "parsePipe" );
    RegexGenerator splitter=new RegexGenerator(new Fields("json"),"^\\{\"Envelope\".*$");
    //Each  operator will filter each entry in the Tuple stream depending on the Regex definition:
    parsePipe = new Each( parsePipe, new Fields( "line" ), splitter, Fields.RESULTS );

    // connect the taps, pipes, etc., into a flow
    FlowDef flowDef = FlowDef.flowDef()
     .addSource( parsePipe, inTap )
     .addTailSink( parsePipe, outTap );

    // run the flow
    flowConnector.connect( flowDef ).complete();
    }
  }
      
