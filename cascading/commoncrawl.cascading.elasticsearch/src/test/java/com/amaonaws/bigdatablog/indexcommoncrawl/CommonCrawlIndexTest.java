package com.amaonaws.bigdatablog.indexcommoncrawl;

import org.junit.Before;
import org.junit.Test;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import java.io.IOException;
import java.util.Properties;
import com.amazonaws.bigdatablog.indexcommoncrawl.ConfigReader;
import com.amazonaws.bigdatablog.indexcommoncrawl.CommonCrawlIndex;


public class CommonCrawlIndexTest {

    @Before
    public void doNotCareAboutOsStuff() {
        System.setProperty("line.separator", "\n");
    }

    @Test
    public void testMain() throws Exception {

        Properties properties = null;
        try {
            properties = new ConfigReader().renderProperties(CommonCrawlIndexTest.class);
            if (properties.getProperty("platform").toString().compareTo("LOCAL")==0){
                FlowDef flowDef = CommonCrawlIndex.buildFlowDef(properties);
                //Using cascading Local connector to exclude Hadoop and just test the logic
                new LocalFlowConnector(properties).connect(flowDef).complete();
            }
        } catch (IOException e) {
            System.out.println("Could not read your config.properties file");e.printStackTrace();
        }
    }
}

