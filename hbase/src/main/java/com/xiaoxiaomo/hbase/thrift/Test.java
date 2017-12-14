package com.xiaoxiaomo.hbase.thrift;

import org.apache.hadoop.hbase.thrift.generated.IOError;
import org.apache.hadoop.hbase.thrift.generated.IllegalArgument;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.*;

public class Test {

    private static final String CHARSET = "UTF-8";
    static DecimalFormat formatter = new DecimalFormat("00");
    private final AbstractHBaseThriftService client;

    public Test(String host, int port) {
        client = new HBaseThriftClient(host, port);
        try {
            client.open();
        } catch (TTransportException e) {
            e.printStackTrace();
        }
    }

    public Test() {
        this("fetch-slave3", 9090);
    }

    static String randomlyBirthday() {
        Random r = new Random();
        int year = 1900 + r.nextInt(100);
        int month = 1 + r.nextInt(12);
        int date = 1 + r.nextInt(30);
        return String.valueOf(year + "-" + formatter.format(month) + "-" + formatter.format(date));
    }

    static String randomlyGender() {
        Random r = new Random();
        int flag = r.nextInt(2);
        return flag == 0 ? "M" : "F";
    }

    static String randomlyUserType() {
        Random r = new Random();
        int flag = 1 + r.nextInt(10);
        return String.valueOf(flag);
    }

    static ByteBuffer wrap(String value) {
        ByteBuffer bb = null;
        try {
            bb = ByteBuffer.wrap(value.getBytes(CHARSET));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return bb;
    }

    static DecimalFormat rowKeyFormatter = new DecimalFormat("0000");

    public void caseForUpdate() throws TException {
        boolean writeToWal = false;
        Map<String, String> attributes = new HashMap<String, String>(0);
        String table = setTable();
        // put kv pairs
        for (long i = 0; i < 10000; i++) {
            String rowKey = rowKeyFormatter.format(i);
            Map<String, String> fieldNameValues = new HashMap<String, String>();
            fieldNameValues.put("info:birthday", randomlyBirthday());
            fieldNameValues.put("info:user_type", randomlyUserType());
            fieldNameValues.put("info:gender", randomlyGender());
            client.update(table, rowKey, writeToWal, fieldNameValues, attributes);
        }
    }

    public void caseForDeleteCells() throws TException {
        boolean writeToWal = false;
        Map<String, String> attributes = new HashMap<String, String>(0);
        String table = setTable();
        // put kv pairs
        for (long i = 5; i < 10; i++) {
            String rowKey = rowKeyFormatter.format(i);
            List<String> columns = new ArrayList<String>(0);
            columns.add("info:birthday");
            client.deleteCells(table, rowKey, writeToWal, columns, attributes);
        }
    }

    private String setTable() {
        String table = "HServiceTest";
        return table;
    }

    public void caseForDeleteRow() throws TException {
        Map<String, String> attributes = new HashMap<String, String>(0);
        String table = setTable();
        // delete rows
        for (long i = 5; i < 10; i++) {
            String rowKey = rowKeyFormatter.format(i);
            client.deleteRow(table, rowKey, attributes);
        }
    }

    public void caseForScan() throws TException {
        Map<String, String> attributes = new HashMap<String, String>(0);
        String table = setTable();
        String startRow = "0005";
        String stopRow = "0015";
        List<String> columns = new ArrayList<String>(0);
        columns.add("d:name");
        int id = client.scannerOpen(table, startRow, stopRow, columns, attributes);
        int nbRows = 2;
        List<TRowResult> results = client.scannerGetList(id, nbRows);
        while(results != null && !results.isEmpty()) {
            for(TRowResult result : results) {
                client.iterateResults(result);
            }
            results = client.scannerGetList(id, nbRows);
        }
        client.scannerClose(id);
    }

    public void caseForGet() throws TException {
        Map<String, String> attributes = new HashMap<String, String>(0);
        String table = setTable();
        List<String> rows = new ArrayList<String>(0);
        rows.add("0009");
        rows.add("0098");
        rows.add("0999");
        List<String> columns = new ArrayList<String>(0);
        columns.add("info:birthday");
        columns.add("info:gender");
        List<TRowResult> results = client.getRowsWithColumns(table, rows, columns, attributes);
        for(TRowResult result : results) {
            client.iterateResults(result);
        }
    }

    public static void main(String[] args)
            throws IOError, IllegalArgument, TException, UnsupportedEncodingException {
        Test test = new Test();
//		test.caseForUpdate(); // insert or update rows/cells
//		test.caseForDelete(); // delete cells
//		test.caseForDeleteRow(); // delete rows
		test.caseForScan(); // scan rows
//        test.caseForGet(); // get rows
    }

}
