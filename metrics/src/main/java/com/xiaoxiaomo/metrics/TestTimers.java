package com.xiaoxiaomo.metrics;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import com.yammer.metrics.reporting.ConsoleReporter;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 *
 * 时间统计
 *
 * Created by TangXD on 2017/7/25.
 */
public class TestTimers {

    private static Timer timer = Metrics.newTimer(TestTimers.class,"responses", TimeUnit.MICROSECONDS,TimeUnit.SECONDS) ;

    public static void main(String[] args) throws InterruptedException {
        ConsoleReporter.enable(2,TimeUnit.SECONDS);
        Random random = new Random();
        timer.time();
        System.out.println();
        while ( true ){
            TimerContext context = timer.time();
            Thread.sleep(random.nextInt(1000));
            context.stop();
        }
    }



}
