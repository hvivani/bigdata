package com.amazonaws.vivanih.hadoop.cascading;

import java.util.Properties;
import java.util.Calendar;
import java.text.SimpleDateFormat;

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
    Scheme outScheme = new TextDelimited( new Fields( /*"year",*/ "month", "UPBaverageFormatted"),"\t" ) ;

    // create source and sink taps
    Tap inTap = new Hfs( inScheme, inPath );
    Tap outTap = new Hfs( outScheme, outPath );
    //Tap outTap = new Hfs( new TextDelimited(true, "\t"), outPath );

    //convert date to "ts" from MRP field
    DateParser dateParser = new DateParser( new Fields( "ts" ), "MM/dd/yyyy" );

    //Parsing the date. (0 is January)
    //DateParser dateParser= new DateParser(new Fields("month", "day", "year"), new int[] { Calendar.MONTH, Calendar.DAY_OF_MONTH, Calendar.YEAR }, "MM/dd/yyyy");
    //parse pipe will parse the MRP field into month, day, year. /with Fields.ALL I get original fields + new fields. With Fields.RESULTS, I get only the date parsed.
    Pipe parsePipe = new Each("parsePipe", new Fields("MRP"), dateParser , Fields.ALL); 

    //change the format from "ts" to date required
    DateFormatter formatter = new DateFormatter( new Fields( "date" ), "dd/MMMM/yyyy" );
    parsePipe = new Each( parsePipe, new Fields( "ts" ), formatter, Fields.ALL );
 
    //regex to extract the month in MMMM format:
    RegexGenerator splitter=new RegexGenerator(new Fields("month"),"(?<!\\pL)(?=\\pL)[^ ]*(?<=\\pL)(?!\\pL])");
    parsePipe = new Each( parsePipe, new Fields( "date" ), splitter, Fields.ALL );

    //filterning tuples with null at UPB field
    RegexFilter nullfilter = new RegexFilter( "^$" ,true);
    parsePipe = new Each( parsePipe, new Fields( "UPB" ), nullfilter );

    //Pipe to filter duplicates. It is like select distinct. It is like using combiners.
    //http://docs.cascading.org/cascading/2.2/javadoc/cascading/pipe/assembly/Unique.html
    parsePipe = new Unique(parsePipe, new Fields("LoadId", "MRP", "ServicerName", "CIR", "UPB", "LoanAge", "RMLM" , "ARMM", "MadurityDate", "MSA", "CLDS", "ModificationFlag", "ZBC", "ZBED", "RepurchaseIndicator"));

    // aggregate by year, month. input is previous parsePipe
    Pipe averagePipe = new GroupBy( "averagePipe", parsePipe, new Fields(/*"year",*/ "month" )); //,true for descending order
    // average each aggregation
    averagePipe = new Every(averagePipe, new Fields("UPB"), new Average(new Fields("UPBaverage")), Fields.ALL );

    //field formatter: $ xxxxxxx.xx
    FieldFormatter fieldformatter = new FieldFormatter ( new Fields("UPBaverageFormatted")," $ %.2f");
    averagePipe = new Each( averagePipe, new Fields( "UPBaverage" ), fieldformatter, Fields.ALL );

 
    // connect the taps, pipes, etc., into a flow
    FlowDef flowDef = FlowDef.flowDef()
    .addSource( averagePipe, inTap )
    //.addSource( parsePipe, inTap )
    .addTailSink(averagePipe, outTap )
    //.addTailSink(parsePipe, outTap );
    .setName("vivanih's-job-is-running");
    // run the flow
    flowConnector.connect( flowDef ).complete();
    }
  }
