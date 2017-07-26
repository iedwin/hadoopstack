package com.xiaoxiaomo.metrics;

import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;

import java.util.Map;
import java.util.Random;

/**
 *
 * 健康检查
 *
 * Created by xiaoxiaomo on 17-7-26.
 */
public class TestHealthCheck {

    public static void main(String[] args) {

        HealthCheckRegistry healthCheck = new HealthCheckRegistry();

        healthCheck.register("healthCheck job " , new DatabaseHealthCheck());

        while ( true ) {
            final Map<String, HealthCheck.Result> results = healthCheck.runHealthChecks();
            for (Map.Entry<String, HealthCheck.Result> entry : results.entrySet()) {
                if (entry.getValue().isHealthy()) {
                    System.out.println(entry.getKey() + " is healthy");
                } else {
                    System.err.println(entry.getKey() + " is UNHEALTHY: " + entry.getValue().getMessage());
                    final Throwable e = entry.getValue().getError();
                    if (e != null) {
                        e.printStackTrace();
                    }
                }
            }

        }

    }

    static class DatabaseHealthCheck extends HealthCheck{

        Database database = new Database();
        @Override
        protected Result check() throws Exception {
            if (database.isConnected()) {
                return HealthCheck.Result.healthy();
            } else {
                return HealthCheck.Result.unhealthy("Cannot connect to ");
            }
        }
    }


    static class Database{
        Random rn = new Random();
        public boolean isConnected() {
            return rn.nextBoolean();
        }
    }


}
