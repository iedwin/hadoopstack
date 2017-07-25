package com.xiaoxiaomo.metrics;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.reporting.ConsoleReporter;

import java.util.concurrent.TimeUnit;

/**
 *
 * Histograms 直方图
 * 最大值，最小值，平均值，方差，中位值，百分比数据，如75%,90%,98%,99%的数据在哪个范围内
 *
 * Created by TangXD on 2017/7/25.
 */
public class TestHistograms {

    private static Histogram histogram = Metrics.newHistogram(TestHistograms.class , "histogram-sizes") ;

    public static void main(String[] args) throws InterruptedException {

        ConsoleReporter.enable(1, TimeUnit.SECONDS);

        int i = 0 ;
        while ( true ){
            histogram.update(i++);
            Thread.sleep(1000);
        }


    }

}
