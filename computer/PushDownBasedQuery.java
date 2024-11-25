package computer;


import event.CrimesEvent;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import query.QueryParse;
import rpc.iface.PushDownRPC;
import store.EventSchema;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PushDownBasedQuery {
    public static int maxMassageLen = 512 * 1024 * 1024 + 100;
    public static int recursionLimit = 64;

    static class PushDownThread extends Thread {
        private final PushDownRPC.Client client;
        private final String tableName;
        // ip: independent predicate
        private final Map<String, List<String>> ipMap;
        private final int recordLen;
        private List<byte[]> events;

        public PushDownThread(PushDownRPC.Client client, String tableName, Map<String, List<String>> ipMap, int recordLen){
            this.client = client;
            this.tableName = tableName;
            this.ipMap = ipMap;
            this.recordLen = recordLen;
        }

        public List<byte[]> getEvents(){
            return events;
        }

        @Override
        public void run() {
            try {
                ByteBuffer buffer = client.initial(tableName, ipMap);
                int capacity = buffer.remaining();
                int recordNum = capacity / recordLen;
                events = new ArrayList<>(recordNum);
                for(int i = 0; i < recordNum; ++i){
                    byte[] record = new byte[recordLen];
                    buffer.get(record);
                    events.add(record);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static List<byte[]> communicate(String sql, List<PushDownRPC.Client> clients){
        QueryParse query = new QueryParse(sql);
        String tableName = query.getTableName();
        Map<String, List<String>> ipMap = query.getIpStringMap();

        int nodeNum = clients.size();
        EventSchema schema = EventSchema.getEventSchema(tableName);
        int dataLen = schema.getFixedRecordLen();

        List<PushDownThread> initialThreads = new ArrayList<>(nodeNum);
        for (PushDownRPC.Client client : clients) {
            PushDownThread thread = new PushDownThread(client, tableName, ipMap, dataLen);
            thread.start();
            initialThreads.add(thread);
        }
        for (PushDownThread t : initialThreads) {
            try{
                t.join();
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        List<byte[]> filteredEvent = null;
        for(PushDownThread t : initialThreads){
            if(filteredEvent == null){
                filteredEvent = t.getEvents();
            }else{
                filteredEvent.addAll(t.getEvents());
            }
        }
        return filteredEvent;
    }

    public static void testCrimesDataset(List<PushDownRPC.Client> clients){
        Schema schema = Schema.newBuilder()
                .column("type", DataTypes.STRING())
                .column("id", DataTypes.INT())
                .column("beat", DataTypes.INT())
                .column("district", DataTypes.INT())
                .column("latitude", DataTypes.DOUBLE())
                .column("longitude", DataTypes.DOUBLE())
                .column("eventTime", DataTypes.TIMESTAMP(3))
                .watermark("eventTime", "eventTime - INTERVAL '0' SECOND")
                .build();

        String sql =
                "SELECT * FROM Crimes MATCH_RECOGNIZE(\n" +
                "    ORDER BY eventTime\n" +
                "    MEASURES A.id as AID, B.id as BID, C.id AS CID\n" +
                "    ONE ROW PER MATCH\n" +
                "    AFTER MATCH SKIP TO NEXT ROW \n" +
                "    PATTERN (A N1*? B N2*? C) WITHIN INTERVAL '31' MINUTE \n" +
                "    DEFINE \n" +
                "        A AS A.type = 'ROBBERY' AND A.beat >= 1900 AND A.beat <= 2000, \n" +
                "        B AS B.type = 'BATTERY', \n" +
                "        C AS C.type = 'MOTOR_VEHICLE_THEFT' AND C.district = B.district \n" +  //AND C.district = B.district
                ") MR;";

        long startTime = System.currentTimeMillis();
        List<byte[]> byteRecords = communicate(sql, clients);
        // we use below statements to generate output
        List<CrimesEvent> events = byteRecords.stream().map(CrimesEvent::valueOf).collect(Collectors.toList());
        long endTime = System.currentTimeMillis();
        System.out.println("========>pull event time: " + (endTime - startTime) + "ms");
        // please modify below table name
        ProcessQueryByFlink.processQuery(events, schema, sql, "Crimes");
    }

    // testXXXDataset()...
    public static void main(String[] args) throws Exception {
        TConfiguration conf = new TConfiguration(maxMassageLen, maxMassageLen, recursionLimit);
        String[] storageNodeIps = {"localhost"};
        int[] ports = {9090};
        int nodeNum = storageNodeIps.length;
        int timeout = 0;
        List<TTransport> transports = new ArrayList<>(nodeNum);
        List<PushDownRPC.Client> clients = new ArrayList<>(nodeNum);

        for(int i = 0; i < nodeNum; i++) {
            TSocket socket = new TSocket(conf, storageNodeIps[i], ports[i], timeout);
            TTransport transport = new TFramedTransport(socket, maxMassageLen);
            TProtocol protocol = new TBinaryProtocol(transport);
            PushDownRPC.Client client = new PushDownRPC.Client(protocol);
            // when we open, we can call related interface
            transport.open();
            clients.add(client);
            transports.add(transport);
        }

        int loop = 5;
        for(int i = 0; i < loop; i++){
            long startTime = System.currentTimeMillis();
            testCrimesDataset(clients);
            long endTime = System.currentTimeMillis();
            System.out.println("========>query cost: " + (endTime - startTime) + "ms");
            System.out.println("------------------------------------");
        }

        System.out.println("--finish--");
        // we need to close transport
        for(TTransport transport : transports){
            transport.close();
        }
    }
}
