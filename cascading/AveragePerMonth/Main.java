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
import cascading.scheme.Scheme;
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

    //scheme definition:
    Scheme inScheme = new TextDelimited( new Fields("LoadId", "MRP", "ServicerName", "CIR", "UPB", "LoanAge", "RMLM" , "ARMM", "MadurityDate", "MSA", "CLDS", "ModificationFlag", "ZBC", "ZBED", "RepurchaseIndicator"), "|");
    Scheme outScheme = new TextDelimited( new Fields( "MRP", "UPB" ) );

    // create source and sink taps
    Tap inTap = new Hfs( inScheme, inPath );
    Tap outTap = new Hfs( outScheme, outPath );


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
