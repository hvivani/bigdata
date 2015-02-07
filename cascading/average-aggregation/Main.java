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

    //Scheme definition: I have started defining the format directly at the In/Out Tap (1). Then discovered that I can personalize the output with Schemes.
    Scheme inScheme = new TextDelimited( new Fields("LoadId", "MRP", "ServicerName", "CIR", "UPB", "LoanAge", "RMLM" , "ARMM", "MadurityDate", "MSA", "CLDS", "ModificationFlag", "ZBC", "ZBED", "RepurchaseIndicator"), "|");
    Scheme outScheme = new TextDelimited( new Fields( /*"year",*/ "month", "UPBaverageFormatted"),"\t" ) ;

    // create source and sink taps
    Tap inTap = new Hfs( inScheme, inPath );
    Tap outTap = new Hfs( outScheme, outPath );
    /*(1)*///Tap outTap = new Hfs( new TextDelimited(true, "\t"), outPath ); 

	//I've started trying to parse the date directly (2), but I couldn't get the month name. So I am transforming to time stamp, to be able to use Date Formatter.
    //convert date to "ts" from MRP field
    DateParser dateParser = new DateParser( new Fields( "ts" ), "MM/dd/yyyy" );

    /*(2)*///Parsing the date. (0 is January)
    //DateParser dateParser= new DateParser(new Fields("month", "day", "year"), new int[] { Calendar.MONTH, Calendar.DAY_OF_MONTH, Calendar.YEAR }, "MM/dd/yyyy");
    //parse pipe will parse the MRP field into month, day, year. 
	//with Fields.ALL I get original fields + new fields. With Fields.RESULTS, I get only the date parsed (result for this query)
    Pipe parsePipe = new Each("parsePipe", new Fields("MRP"), dateParser , Fields.ALL); 

    /*(3)*///change the format from "ts" to date required. Month name is MMMM.
    DateFormatter formatter = new DateFormatter( new Fields( "date" ), "dd/MMMM/yyyy" );
    parsePipe = new Each( parsePipe, new Fields( "ts" ), formatter, Fields.ALL );
 
    //I am cascading the filters, one after other:
	
    /*(4)*///regex to extract the month in MMMM format.
    RegexGenerator splitter=new RegexGenerator(new Fields("month"),"(?<!\\pL)(?=\\pL)[^ ]*(?<=\\pL)(?!\\pL])");
    parsePipe = new Each( parsePipe, new Fields( "date" ), splitter, Fields.ALL );

	
    /*(5)*///filtering tuples with null at UPB field. After comparing the results with Hive query, I have had to find differences. All of them were related to the NULL or empty fields 
	//at UPB Field. To resolve the issue i have performed counts and sums separated on Cascading and Hive.
	//Cascading is counting the nulls as empty string on average or count function. Hive is no counting when null.
    RegexFilter nullfilter = new RegexFilter( "^$" ,true);
    parsePipe = new Each( parsePipe, new Fields( "UPB" ), nullfilter );

    /*(6)*///Pipe to filter duplicates. It is like select distinct. 
    //I am checking complete row as duplicate. Hive also has the DISTINCT that applies to all the rows. Not selective rows.
    parsePipe = new Unique(parsePipe, new Fields("LoadId", "MRP", "ServicerName", "CIR", "UPB", "LoanAge", "RMLM" , "ARMM", "MadurityDate", "MSA", "CLDS", "ModificationFlag", "ZBC", "ZBED", "RepurchaseIndicator"));

    /*(7)*/// aggregate by month. input in previous parsePipe.
	//It was making more sense for me to group by year and month. Finally, applied only the month as requested.
    Pipe averagePipe = new GroupBy( "averagePipe", parsePipe, new Fields(/*"year",*/ "month" )); //,true for descending order
    // average each aggregation. Every will apply the function (Average) to each grouped field (month).
    averagePipe = new Every(averagePipe, new Fields("UPB"), new Average(new Fields("UPBaverage")), Fields.ALL );

    /*(8)*///field formatter: $ xxxxxxx.xx
	//Was difficult to find the way to format the output of a field. Not too much documents on it. Finally, I have deduced that this function is using Java formatting.
	//(Docs do not clarify about it)
    FieldFormatter fieldformatter = new FieldFormatter ( new Fields("UPBaverageFormatted")," $ %.2f");
    averagePipe = new Each( averagePipe, new Fields( "UPBaverage" ), fieldformatter, Fields.ALL );
	
	//Some of these, like the final output format will make the process slower as the presentation stage could be handled outside of current ETL.

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
