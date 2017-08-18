package com.xiaoxiaomo;

import com.xiaoxiaomo.metrics.opentsdb.OpentsdbClient;
import com.xiaoxiaomo.metrics.utils.ConfigLoader;
import com.xiaoxiaomo.metrics.utils.DateTimeUtil;
import org.junit.Test;
import org.opentsdb.client.request.Filter;
import org.opentsdb.client.util.Aggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class OpentsdbClientTest {

    private static Logger log = LoggerFactory.getLogger(OpentsdbClientTest.class);

    @Test
    public void testPutData() {
        OpentsdbClient client = new OpentsdbClient(ConfigLoader.getProperty("opentsdb.url"));
        try {
            Map<String, String> tagMap = new HashMap<String, String>();
            tagMap.put("chl", "hqdApp");

            client.putData("metric-t", DateTimeUtil.str2Date("20160627 12:15", "yyyyMMdd HH:mm"), 210l, tagMap);
            client.putData("metric-t", DateTimeUtil.str2Date("20160627 12:17", "yyyyMMdd HH:mm"), 180l, tagMap);
            client.putData("metric-t", DateTimeUtil.str2Date("20160627 13:20", "yyyyMMdd HH:mm"), 180l, tagMap);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testPutData2() {
        OpentsdbClient client = new OpentsdbClient(ConfigLoader.getProperty("opentsdb.url"));
        try {
            Map<String, String> tagMap = new HashMap<String, String>();
            tagMap.put("chl", "hqdWechat");

            client.putData("metric-t", DateTimeUtil.str2Date("20160627 12:25", "yyyyMMdd HH:mm"), 120l, tagMap);
            client.putData("metric-t", DateTimeUtil.str2Date("20160627 12:27", "yyyyMMdd HH:mm"), 810l, tagMap);
            client.putData("metric-t", DateTimeUtil.str2Date("20160627 13:20", "yyyyMMdd HH:mm"), 880l, tagMap);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetData() {
        OpentsdbClient client = new OpentsdbClient(ConfigLoader.getProperty("opentsdb.url"));
        try {
            Filter filter = new Filter();
            filter.setType("regexp");
            filter.setTagk("chl");
            filter.setFilter("hqdApp");
            filter.setGroupBy(Boolean.TRUE);
            String resContent = client.getData("metric-t", filter, Aggregator.avg.name(), "1h",
                    "2016-06-27 12:00:00", "2016-06-30 13:00:00");

            log.info(">>>" + resContent);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetData2() {
        OpentsdbClient client = new OpentsdbClient(ConfigLoader.getProperty("opentsdb.url"));
        try {
            Filter filter = new Filter();
            String tagk = "chl";
            String tagvFtype = OpentsdbClient.FILTER_TYPE_WILDCARD;
            String tagvFilter = "hqdapp*";

            Map<String, Map<String, Object>> tagsValuesMap = client.getData("metric-t", tagk, tagvFtype, tagvFilter, Aggregator.avg.name(), "1m",
                    "2016-06-27 12:00:00", "2016-06-30 11:00:00", "yyyyMMdd HHmm");

            for (Iterator<String> it = tagsValuesMap.keySet().iterator(); it.hasNext(); ) {
                String tags = it.next();
                System.out.println(">> tags: " + tags);
                Map<String, Object> tvMap = tagsValuesMap.get(tags);
                for (Iterator<String> it2 = tvMap.keySet().iterator(); it2.hasNext(); ) {
                    String time = it2.next();
                    System.out.println("    >> " + time + " <-> " + tvMap.get(time));
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetData3() {
        OpentsdbClient client = new OpentsdbClient(ConfigLoader.getProperty("opentsdb.url"));
        try {
            Filter filter = new Filter();
            filter.setType(OpentsdbClient.FILTER_TYPE_LITERAL_OR);
            filter.setTagk("chl");
            filter.setFilter("hqdApp|hqdWechat");
            filter.setGroupBy(Boolean.TRUE);
            String resContent = client.getData("metric-t", filter, Aggregator.avg.name(), "1h",
                    "2016-06-27 12:00:00", "2016-06-28 00:00:00");

            log.info(">>>" + resContent);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
