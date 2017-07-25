package com.xiaoxiaomo.metrics;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.reporting.ConsoleReporter;

import java.util.concurrent.TimeUnit;

/**
 *
 * Meters会将最近1分钟，5分钟，15分钟的TPS（每秒处理的request数）给打印出来，还有所有时间的TPS。
 *
 * Created by TangXD on 2017/7/25.
 */
public class TestMeters {

    private static Meter meter = Metrics.newMeter(TestMeters.class,"requests","requests", TimeUnit.SECONDS);

    public static void main(String[] args) throws InterruptedException {
        ConsoleReporter.enable(1 , TimeUnit.SECONDS);
        while ( true ){
            meter.mark();
            meter.mark();
            Thread.sleep(1000);
        }
    }

}
