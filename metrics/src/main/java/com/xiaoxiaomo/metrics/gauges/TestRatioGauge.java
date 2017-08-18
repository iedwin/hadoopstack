package com.xiaoxiaomo.metrics.gauges;

import com.codahale.metrics.*;

import java.util.concurrent.TimeUnit;

/**
 *
 * RatioGauge可以计算两个Gauge的比值。 Meter和Timer可以参考下面的代码创建。下面的代码用来计算计算命中率 (hit/call)。
 *
 * Created by xiaoxiaomo 17-7-26.
 */
public class TestRatioGauge extends RatioGauge{

    private static MetricRegistry registry ;
    private static ConsoleReporter reporter ;

    private final Meter hits;
    private final Timer calls;
    public TestRatioGauge(Meter hits, Timer calls) {
        this.hits = hits;
        this.calls = calls;
    }

    static {
        registry = new MetricRegistry();

        reporter = ConsoleReporter.forRegistry(registry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();

        reporter.start(2,TimeUnit.MINUTES);
    }


    @Override
    protected Ratio getRatio() {
        return Ratio.of(hits.getCount(),calls.getCount());
    }


    public static void main(String[] args) {

    }
}
