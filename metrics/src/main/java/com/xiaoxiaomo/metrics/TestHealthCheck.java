package com.xiaoxiaomo.metrics;

import com.yammer.metrics.HealthChecks;
import com.yammer.metrics.core.HealthCheck;

import java.util.Map;
import java.util.Random;

/**
 *
 * 健康检查
 *
 * Created by TangXD on 2017/7/25.
 */
public class TestHealthCheck extends HealthCheck{

    private static Database DATABASE;
    private static final Map<String,Result> resultMap = HealthChecks.runHealthChecks();

    /**
     * Create a new {@link HealthCheck} instance with the given name.
     *
     * @param DATABASE the name of the health check (and, ideally, the name of the underlying
     *             component the health check tests)
     */
    protected TestHealthCheck(Database DATABASE) {
        super("database");
        this.DATABASE = DATABASE ;
    }

    @Override
    protected Result check() throws Exception {
        if( DATABASE.isConnected() ){
            return Result.healthy();
        } else {
            return Result.unhealthy(" 链接失败！ ");
        }
    }


    public static void main(String[] args) throws InterruptedException {
        Database database = new Database();
        TestHealthCheck healthCheck = new TestHealthCheck(database);
        HealthChecks.register(healthCheck);

        while ( true ){
            Map<String, Result> results = HealthChecks.runHealthChecks();
            for (Map.Entry<String, Result> entry : results.entrySet()) {
                if( entry.getValue().isHealthy() ){
                    System.out.println( entry.getKey() + " is healthy " );
                }

                else {
                    System.out.println( entry.getKey() + " is unhealthy " + entry.getValue().getMessage());
                }

            }

            Thread.sleep(1000);
        }


    }
}


class Database{
    static Random rn = new Random();
    public boolean isConnected() {
        return rn.nextBoolean();
    }
}
