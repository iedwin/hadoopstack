package com.xiaoxiaomo.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;


/**
 * Created by TangXD on 2017/12/12.
 */
public class HbaseTest {


    public static void main(String args[]) {
        Connection connection = null;
        Statement statement = null;
        ResultSet rs = null ;
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            connection = DriverManager.getConnection("jdbc:phoenix:fetch-slave1:2181","","");
            statement = connection.createStatement();
            String sql = "select count(1) as num from \"HServiceTest\"";
            long time = System.currentTimeMillis();
            rs = statement.executeQuery(sql);
            while (rs.next()) {
                int count = rs.getInt("num");
                System.out.println("row count is " + count);
            }
            long timeUsed = System.currentTimeMillis() - time;
            System.out.println("time " + timeUsed + "mm");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                rs.close();
                connection.close();
                statement.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
