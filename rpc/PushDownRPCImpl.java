package rpc;

import org.apache.thrift.TException;
import rpc.iface.PushDownRPC;
import store.FullScan;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * it only uses independent predicates to filter
 * one-pass communication
 */
public class PushDownRPCImpl implements PushDownRPC.Iface {
    @Override
    public ByteBuffer initial(String tableName, Map<String, List<String>> ipMap) throws TException {
        long startTime = System.currentTimeMillis();
        FullScan fullscan = new FullScan(tableName);
        List<byte[]> filteredRecords = fullscan.scan(ipMap);

        long endTime = System.currentTimeMillis();
        System.out.println("scan cost: " + (endTime - startTime) + "ms");

        if(filteredRecords.isEmpty()) {
            return null;
        }else{
            int size = filteredRecords.size();
            int dataLen = filteredRecords.get(0).length;
            ByteBuffer buffer = ByteBuffer.allocate(size * dataLen);
            for (byte[] record : filteredRecords) {
                buffer.put(record);
            }
            buffer.flip();
            return buffer;
        }
    }
}
