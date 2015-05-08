/* The MIT License (MIT)

Copyright (c) 2014 Concurrent Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE. */

/* @author Alexis Roos @alexisroos */



// Lab 2 - Tuples, Fields and Coercion
// ===================================
//
// This lab builds upon part 1 using a TSV log file to add mapping of tuples to fields and using coercion to convert a date field.

// You will be using the following classes to complete the lab:

// CoercibleType: http://docs.cascading.org/cascading/2.5/javadoc/cascading/tuple/type/CoercibleType.html

// DateType: http://docs.cascading.org/cascading/2.5/javadoc/cascading/tuple/type/DateType.html

// Fields: http://docs.cascading.org/cascading/2.5/javadoc/cascading/tuple/Fields.html

// FileTap: http://docs.cascading.org/cascading/2.5/javadoc/cascading/tap/local/FileTap.html

// TextDelimited: http://docs.cascading.org/cascading/2.5/javadoc/cascading/scheme/local/TextDelimited.html


// Task 1 - Examine provided code
// ------------------------------

package devtraining;

import cascading.flow.Flow;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.type.CoercibleType;
import cascading.tuple.type.DateType;

import java.util.Properties;
import java.util.TimeZone;

public class
        Main {
    public static void main(String[] args) {

        // Input file
        String inputPath = args[0];
        // Output file
        String outputPath = args[1];
        
        //Task 2 - Specify the input File
        //-------------------------------
        // The file is in TSV format:

        // in24.inetnebr.com [01/Aug/1995:00:00:01 -0400] "GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0" 200 1839

        // Step 1 - Declare CoercibleType
        // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        // Use "[dd/MMM/yyyy:HH:mm:ss Z]" for data formatting with DateType class
        CoercibleType coercible = new DateType("[dd/MMM/yyyy:HH:mm:ss Z]", TimeZone.getDefault());

        // Step 2 - Declare log file field names
        // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        // Declares the field names used to parse out of the log file as "ip", "time", "request", "response" and "size".
        // Use coercion with the following types: String, coercible type as defined in step 1, String, long, long
        Fields apacheFields = new Fields("ip", "time", "request", "response", "size").applyTypes(String.class, coercible, String.class, long.class, long.class);
        // Task 3 - Create In and Out Taps
        // -------------------------------
        // Step 1 - Create an inTap
        // ~~~~~~~~~~~~~~~~~~~~~~~~
        // Create a source tap, "inTap", using TextDelimited scheme (TAB default) using fields declared above.
        // Use the input path name specified in the code
        Tap inTap = new FileTap(new TextDelimited(apacheFields), inputPath);
        // Step 2 - Create an outTap
        // ~~~~~~~~~~~~~~~~~~~~~~~~~
        // Create a sink tap, "outTap", to write to the default filesystem; by default; use SinkMode REPLACE
        // Use the output path name specified in the code
        Tap outTap = new FileTap(new TextDelimited(), outputPath, SinkMode.REPLACE);

        // Task 4 - Examine Code Provided
        // ------------------------------
        // Creates the copyPipe pipe element, with the name 'Copy'
        Pipe copyPipe = new Pipe("Copy Part2");

        // Create a local planner for executing the flow
        Properties properties = AppProps.appProps()
                .setName("part2")
                .buildProperties();

        // Connect the assembly to the SOURCE and SINK taps
        Flow parsedLogFlow = new LocalFlowConnector(properties).connect(inTap, outTap, copyPipe);
        // Run the flow
        parsedLogFlow.complete();
    }
}
