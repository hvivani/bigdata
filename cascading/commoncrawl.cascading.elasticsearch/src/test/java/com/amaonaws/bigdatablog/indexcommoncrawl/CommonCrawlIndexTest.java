package com.amaonaws.bigdatablog.indexcommoncrawl;

import org.junit.Before;
import org.junit.Test;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
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
        Properties properties = new ConfigReader().renderProperties(CommonCrawlIndexTest.class);
        FlowDef flowDef = CommonCrawlIndex.buildFlowDef(properties);
        //Using cascading Local connector to exclude Hadoop and just test the logic
        new LocalFlowConnector(properties).connect(flowDef).complete();
    }


}
