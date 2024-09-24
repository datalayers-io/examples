package org.example;

import org.apache.arrow.flight.*;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SqlRunner {

    private static final Logger log = LoggerFactory.getLogger(SqlRunner.class);

    static void run_flight_sql() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
            final Location clientLocation = Location.forGrpcInsecure("127.0.0.1", 8360);
            try (FlightClient client = FlightClient.builder(allocator, clientLocation).build();
                 FlightSqlClient sqlClient = new FlightSqlClient(client)) {

                Optional<CredentialCallOption> credentialCallOption = client.authenticateBasicToken("admin", "public");
                CallHeaders headers = new FlightCallHeaders();
                headers.insert("database", "test");

                Set<CallOption> options = new HashSet<>();
                credentialCallOption.ifPresent(options::add);
                options.add(new HeaderCallOption(headers));
                try {
                    String query = "create database test";
                    executeQuery(sqlClient, query, options);
                } catch (Exception e){
                    e.printStackTrace();
                    throw e;
                }

                try {
                    String query = "CREATE TABLE test.demo (" +
                    "ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                    "sid INT32," +
                    "value REAL," +
                    "flag INT8,"  +
                    "timestamp key(ts)" +
                    ")" +
                    "PARTITION BY HASH(sid) PARTITIONS 8" +
                    "ENGINE=TimeSeries";
                    executeQuery(sqlClient, query, options);
                } catch (Exception e){
                    e.printStackTrace();
                    throw e;
                }


                try {
                    String query = "INSERT INTO test.demo (sid, value, flag) VALUES (1, 1.1, 1);";
                    executeQuery(sqlClient, query, options);
                } catch (Exception e){
                    e.printStackTrace();
                    throw e;
                }

               try {
                   String query = "SELECT count(*) from test.demo;";
                   executeQuery(sqlClient, query, options);
               } catch (Exception e){
                   e.printStackTrace();
                   throw e;
               }
            }
        }
    }

    private static void executeQuery(FlightSqlClient sqlClient, String query, Set<CallOption> options) throws Exception {
        final FlightInfo info = sqlClient.execute(query, options.toArray(new CallOption[0]));
        final Ticket ticket = info.getEndpoints().get(0).getTicket();
        try (FlightStream stream = sqlClient.getStream(ticket, options.toArray(new CallOption[0]))) {
            while (stream.next()) {
                try (VectorSchemaRoot schemaRoot = stream.getRoot()) {
//                    // How to get single element
//                    // You can cast the FieldVector class to some class Like TinyIntVector and so on.
//                    // You can get the type mapping from arrow official website
//                    List<FieldVector> vectors = schemaRoot.getFieldVectors();
//                    for (int i = 0; i < vectors.size(); i++) {
//                        System.out.printf("Col :%d %s\n", i, vectors.get(i));
//                    }
                    log.info(schemaRoot.contentToTSVString());
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        run_flight_sql();
    }
}
