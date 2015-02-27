package com.amazonaws.vivanih.hadoop.cascading;

import java.util.Properties;

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
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import org.elasticsearch.hadoop.cascading.EsTap;


public class
  Main
  {
  public static void
  main( String[] args )
    {
    String inPath = args[ 0 ];
    String outPath = args[ 1 ];

    Properties properties = new Properties();
    AppProps.setApplicationJarClass( properties, Main.class );
   // AppProps.setApplicationJarPath(properties, "/home/hadoop/impatient.jar");
    //FlowConnector flowConnector = new Hadoop2MR1FlowConnector( properties );
    //test elasticsearch set property
    //props.setProperty("es.index.auto.create", "false");

    FlowConnector flowConnector = new HadoopFlowConnector( properties );

    // create the source tap
    //Tap inTap = new Hfs( new TextDelimited( true, "\t" ), inPath );
    Tap inTap = new Lfs(new TextDelimited(new Fields("id", "name", "url", "picture")), "/mnt/data/");

    // create the sink tap
    //Tap outTap = new Hfs( new TextDelimited( true, "\t" ), outPath );
    Tap outTap = new EsTap("radio/artists" , new Fields("name", "url", "picture") );

    // specify a pipe to connect the taps
    Pipe copyPipe = new Pipe( "write-to-Es" );

    // connect the taps, pipes, etc., into a flow
    FlowDef flowDef = FlowDef.flowDef()
     .addSource( copyPipe, inTap )
     .addTailSink( copyPipe, outTap );

    // run the flow
    flowConnector.connect( flowDef ).complete();
    }
  }
