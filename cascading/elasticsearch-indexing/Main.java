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
    String outPath = args[ 1 ];
    //String trapPath = args[ 1 ];

    Properties properties = new Properties();
    AppProps.setApplicationJarClass( properties, Main.class );
    //we tell cascading we are using JSON files as input:
    properties.setProperty("es.input.json", "true");
    //disable speculative execution
    properties.setProperty("mapreduce.map.speculative", "false");
    properties.setProperty("mapreduce.reduce.speculative", "false");
    properties.setProperty("es.port", "9202");

   // AppProps.setApplicationJarPath(properties, "/home/hadoop/impatient.jar");
    //FlowConnector flowConnector = new Hadoop2MR1FlowConnector( properties );
    //test elasticsearch set property
    //props.setProperty("es.index.auto.create", "false");

    FlowConnector flowConnector = new HadoopFlowConnector( properties );
    //FlowConnector flowConnector = new Hadoop2MR1FlowConnector( properties );

    // create the source tap for JSON input
    Tap inTap = new Hfs( new TextLine(new Fields("line")), inPath );
    //Tap trapTap = new Hfs( new TextLine(new Fields("line")), trapPath );
    //Tap inTap = new Lfs(new TextDelimited(new Fields("id", "name", "url", "picture")), inPath);

    // create the sink tap
    //Tap outTap = new Hfs( new TextLine(new Fields("line")), outPath );
    Tap outTap = new EsTap("radio/artists");

    Pipe parsePipe = new Pipe( "parsePipe" );
    RegexGenerator splitter=new RegexGenerator(new Fields("json"),"^\\{\"Envelope\".*$");
    parsePipe = new Each( parsePipe, new Fields( "line" ), splitter, Fields.RESULTS );

    // specify a pipe to connect the taps
    //Pipe copyPipe = new Pipe( "write-to-Es" );

    // connect the taps, pipes, etc., into a flow
    FlowDef flowDef = FlowDef.flowDef()
     .addSource( parsePipe, inTap )
     //.addTrap( copyPipe, trapTap )
     .addTailSink( parsePipe, outTap );

    // run the flow
    flowConnector.connect( flowDef ).complete();
    }
  }
      
