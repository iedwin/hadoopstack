package com.xiaoxiaomo.cassandra;

import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

public class CassandraClient {

    public static void main(String[] args) throws TTransportException, UnsupportedEncodingException, InvalidRequestException, NotFoundException, UnavailableException, TimedOutException, TException, AuthenticationException, AuthorizationException {

        TTransport tr = new TFramedTransport(new TSocket("127.0.0.1", 9160));
        TProtocol proto = new TBinaryProtocol(tr);

        Cassandra.Client client = new Cassandra.Client(proto);
        tr.open();
        String keyspace = "testspace";
        client.set_keyspace(keyspace);
        //record id
        String key_user_id = "1";
        String columnFamily = "test1";
        // insert data
        long timestamp = System.currentTimeMillis();
        Random r = new Random(timestamp);
        Column nameColumn = new Column(ByteBuffer.wrap("name".getBytes()));
        nameColumn.setValue(Long.toHexString(r.nextLong()).getBytes());
        nameColumn.setTimestamp(timestamp);

        Column ageColumn = new Column(ByteBuffer.wrap("id".getBytes()));
        ageColumn.setValue(Long.toHexString(r.nextLong()).getBytes());
        ageColumn.setTimestamp(timestamp);

        ColumnParent columnParent = new ColumnParent(columnFamily);
        client.insert(ByteBuffer.wrap(key_user_id.getBytes()), columnParent,nameColumn,ConsistencyLevel.ALL) ;
        client.insert(ByteBuffer.wrap(key_user_id.getBytes()), columnParent,ageColumn,ConsistencyLevel.ALL);

        //Gets column by key
        SlicePredicate predicate = new SlicePredicate();
        predicate.setSlice_range(new SliceRange(ByteBuffer.wrap(new byte[0]), ByteBuffer.wrap(new byte[0]), false, 100));
        List<ColumnOrSuperColumn> columnsByKey = client.get_slice(ByteBuffer.wrap(key_user_id.getBytes()), columnParent, predicate, ConsistencyLevel.ALL);
        System.out.println(columnsByKey);


        //Get all keys
        KeyRange keyRange = new KeyRange(100);
        keyRange.setStart_key(new byte[0]);
        keyRange.setEnd_key(new byte[0]);
        List<KeySlice> keySlices = client.get_range_slices(columnParent, predicate, keyRange, ConsistencyLevel.ONE);
        System.out.println(keySlices.size());
        System.out.println(keySlices);
        for (KeySlice ks : keySlices) {
            System.out.println(new String(ks.getKey()));
        }
        tr.close();
    }
}
