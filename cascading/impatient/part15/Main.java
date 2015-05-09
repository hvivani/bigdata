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



// Lab 15 - Cascading with Hadoop
// ==============================
//
// This lab modifies lab 13 to execute in Hadoop instead of local mode and also showcases using Checkpoint.

// You will be using the following classes to complete the lab:

// Checkpoint: http://docs.cascading.org/cascading/2.5/javadoc/cascading/pipe/Checkpoint.html

// Hfs: http://docs.cascading.org/cascading/2.5/javadoc/cascading/tap/hadoop/Hfs.html

// TextDelimited: http://docs.cascading.org/cascading/2.5/javadoc/cascading/scheme/hadoop/TextDelimited.html

// HadoopFlowConnector: http://docs.cascading.org/cascading/2.5/javadoc/cascading/flow/hadoop/HadoopFlowConnector.html


// Task 1 - Examine provided code
// ------------------------------

package devtraining;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexParser;
import cascading.operation.text.DateParser;
import cascading.pipe.*;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.pipe.assembly.Unique;
import cascading.pipe.joiner.InnerJoin;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.util.Properties;

public class
        Main {
    public static void main(String[] args) {

        // Input file
        String inputPath = args[0];
        // Output file
        String outputPath = args[1];

        // Creates a source tap using scheme and local input file
        // Using default TextLine scheme
        Tap inTap = new Hfs(new TextLine(), inputPath);

        // Creates a sink tap to write to the Hfs; by default, TextDelimited writes all fields out
        // For TextDelimited use header = true and delimiter = "\t"
        Tap outTap = new Hfs(new TextDelimited(true, "\t"), outputPath, SinkMode.REPLACE);

        // Declares the field names used to parse out of the log file
        Fields apacheFields = new Fields("ip", "time", "request", "response", "size");

        // Defines the regular expression used to parse the log file
        String apacheRegex = "^([^ ]*) \\S+ \\S+ \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) ([^ ]*).*$";

        // Declares the groups from the above regex. Each group will be given a field name from 'apacheFields'
        int[] allGroups = {1, 2, 3, 4, 5};

        // Creates the parser
        RegexParser parser = new RegexParser(apacheFields, apacheRegex, allGroups);

        // Creates the main import pipe element, and with the input argument named "line"
        // replace the incoming tuple with the parser results
        Pipe regexImport = new Each("regexImport", new Fields("line"), parser, Fields.RESULTS);

        // Applies a text parser to create a timestamp from date and replace date by this timestamp
        DateParser dateParser = new DateParser(new Fields("time"), "dd/MMM/yyyy:HH:mm:ss Z");
        Pipe transformPipe = new Each(regexImport, new Fields("time"), dateParser, Fields.REPLACE);

        // We want look for ips who have logged in last week and have accessed "GET /images/",
        // (in reality URL would represent particular products or content in shopping carts, etc.)
        // and do a join with our preferred ip list (using their score) which we will generate in a new branch

        // 1st branch: filter out ips who have logged in within a week and accessed GET /images/
        // "GET /images/"
        Pipe filteredPipe = new Pipe("filteredPipe", transformPipe);
        filteredPipe = new Each(filteredPipe, new Fields("request"), new RegexFilter("GET /images/"));
        filteredPipe = new Retain(filteredPipe, new Fields("ip"));
        filteredPipe = new Unique(filteredPipe, new Fields("ip"));

        // We now use a sub assembly for 2nd branch
        UserAssembly userAssembly = new UserAssembly(transformPipe);
        Pipe userPipe = userAssembly.getTails()[0];

        // Task 2 - Create a Checkpoint
        // ----------------------------
        // Step 1 - Create a new Checkpoint (userPipeCheckpoint)
        // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        // We want to see the data passing through this point
        Checkpoint userPipeCheckpoint = new Checkpoint( "userPipeCheckpoint", userPipe );
        // Task 3 - Examine provided code
        // ------------------------------
        // Join both branches on ip address
        Pipe join = new HashJoin(filteredPipe, new Fields("ip"), userPipeCheckpoint, new Fields("userip"), new InnerJoin());
        join = new Discard(join, new Fields("userip"));

        // Group by "score"
        join = new GroupBy(join, new Fields("score"), true);


        // Task 4 - Create a flow
        // ----------------------
        // Step 1 - Set an unique RunID
        // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        // Creates the flow definition by connecting the taps and pipes
        FlowDef flowDef = FlowDef.flowDef()
                .addSource(transformPipe, inTap)
                .addTailSink(join, outTap)
                .setRunID("12345")
                .setName("User Flow - Hadoop");

        // Task 5 - Examine provided code
        // ------------------------------

        // Creates a planner for executing the flow
        Properties properties = AppProps.appProps()
                .setName("part15")
                .buildProperties();

        // Create a Hadoop Flow Connector
        Flow parsedLogFlow = new HadoopFlowConnector(properties).connect(flowDef);

        // Runs the flow
        parsedLogFlow.complete();
    }

    public static class UserAssembly extends SubAssembly {

        public UserAssembly(Pipe pipe) {
            // Registers incoming pipe
            setPrevious(pipe);

            // 2nd branch: add a generated score
            Pipe userPipe = new Pipe("userPipe", pipe);
            // Narrows stream to just IPS, remove duplicates and rename for subsequent join
            userPipe = new Retain(userPipe, new Fields("ip"));
            userPipe = new Unique(userPipe, new Fields("ip"));
            userPipe = new Rename(userPipe, new Fields("ip"), new Fields("userip"));
            // Add a field score generated from userip using hash function
            userPipe = new Each(userPipe, new Fields("userip"), new ScoreNumber(new Fields("score")), Fields.ALL);

            // Finally we filter out ips with score lower than 60
            ExpressionFilter filterScore = new ExpressionFilter("score < 60", Integer.TYPE);
            userPipe = new Each(userPipe, new Fields("score"), filterScore);

            // Registers tails
            setTails(userPipe);
        }

        // Generates a score number between 1 & 100
        public static class ScoreNumber extends BaseOperation implements Function {
            public ScoreNumber() {
                // expects 1 argument, fails otherwise
                super(1, new Fields("score"));
            }

            public ScoreNumber(Fields fieldDeclaration) {
                // expects 1 argument, fails otherwise
                super(1, fieldDeclaration);
            }

            public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
                // gets the arguments TupleEntry
                TupleEntry arguments = functionCall.getArguments();

                // creates a Tuple to hold our result value
                Tuple result = new Tuple();
                String ip = arguments.getString(0);
                int hash = 7;
                for (int i=0; i < ip.length(); i++) {
                    hash = hash*31+ip.indexOf(i);
                }

                int score = hash % 100;
                // adds the score value to the result Tuple
                result.add(score);

                // returns the result Tuple
                functionCall.getOutputCollector().add(result);
            }
        }
    }
}
