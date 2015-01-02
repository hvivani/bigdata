package com.amazonaws.vivanih.hadoop.cascading;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;


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
    FlowConnector flowConnector = new HadoopFlowConnector( properties );

    // create source and sink taps
    Tap inTap = new Hfs( new TextDelimited( false, "|" ), inPath );
    Tap outTap = new Hfs( new TextDelimited( false, "|" ), outPath );

    
    // specify a pipe to connect the taps
    Pipe copyPipe = new Pipe( "copy" );
    // connect the taps, pipes, etc., into a flow
    FlowDef flowDef = FlowDef.flowDef()
    .addSource( copyPipe, inTap )
    .addTailSink( copyPipe, outTap );
    // run the flow
    flowConnector.connect( flowDef ).complete();
    }
  }
