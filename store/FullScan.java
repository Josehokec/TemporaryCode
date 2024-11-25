package store;


import query.IndependentPredicate;
import utils.Pair;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.*;

public class FullScan {
    // if you have multiple storage nodes, please modify this variable
    // it will scan the files from event_store
    private final String tableName;
    private final String nodeId;

    public FullScan(String tableName) {
        this.tableName = tableName;
        nodeId = "";
    }

    // parse independent predicates
    public static Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> parseIpString(Map<String, List<String>> ipStringMap, EventSchema schema) {
        Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap = new HashMap<>(ipStringMap.size() << 2);
        // parse independent predicates
        for(String varName : ipStringMap.keySet()){
            List<String> ipStringList = ipStringMap.get(varName);
            List<Pair<IndependentPredicate, ColumnInfo>> pairs = new ArrayList<>(ipStringList.size());
            for (String ipString : ipStringList){
                IndependentPredicate ip = new IndependentPredicate(ipString);
                pairs.add(new Pair<>(ip, schema.getColumnInfo(ip.getAttributeName())));
            }
            ipMap.put(varName, pairs);
        }
        return ipMap;
    }

    // this function is used for push-down methods
    public List<byte[]> scan(Map<String, List<String>> ipStringMap){
        EventSchema schema = EventSchema.getEventSchema(tableName);
        EventStore store = new EventStore(tableName + nodeId, false);
        List<byte[]> filteredRecords = new ArrayList<>(8192);

        Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap = parseIpString(ipStringMap, schema);

        long fileSize = store.getFileSize();
        int recordLen = schema.getFixedRecordLen();
        File file = store.getFile();

        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            raf.seek(0);
            long bytesRead = 0;
            byte[] buffer = new byte[EventStore.pageSize];
            while (bytesRead < fileSize) {
                int read = raf.read(buffer);
                if (read == -1) break;
                bytesRead += read;
                // start process data
                int curRecordNum = read / recordLen;
                ByteBuffer bb = ByteBuffer.wrap(buffer);
                for(int i = 0; i < curRecordNum; i++){
                    for(Map.Entry<String, List<Pair<IndependentPredicate, ColumnInfo>>> entry : ipMap.entrySet()) {
                        //IndependentPredicate
                        List<Pair<IndependentPredicate, ColumnInfo>> ips = entry.getValue();
                        boolean satisfied = true;
                        for(Pair<IndependentPredicate, ColumnInfo> pair : ips){
                            IndependentPredicate ip = pair.getKey();
                            ColumnInfo columnInfo = pair.getValue();
                            Object obj = schema.getColumnValue(columnInfo, bb, i * recordLen);
                            if(!ip.check(obj, columnInfo.getDataType())){
                                satisfied = false;
                                break;
                            }
                        }
                        if(satisfied){
                            byte[] record = new byte[recordLen];
                            System.arraycopy(buffer, i * recordLen, record, 0, recordLen);
                            filteredRecords.add(record);
                            break;
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return filteredRecords;
    }

    // here we will read all events in batch
    // this function use event store to scan, it has a low query speed, so we discard this function
    // please note that we do not require each variable's result cannot overlap
    @Deprecated
    public EventCache scanBasedVarNameInBatch(Map<String, List<String>> ipStringMap){
        List<byte[]> filteredRecords = new ArrayList<>(1024);
        Map<String, List<Integer>> varPointers = new HashMap<>(ipStringMap.size() << 2);
        Map<String, List<IndependentPredicate>> ipMap = new HashMap<>(ipStringMap.size() << 2);

        for(String varName : ipStringMap.keySet()){
            varPointers.put(varName, new ArrayList<>(128));
            List<String> ipStringList = ipStringMap.get(varName);
            List<IndependentPredicate> ips = new ArrayList<>(ipStringList.size());
            for (String ipString : ipStringList){
                IndependentPredicate ip = new IndependentPredicate(ipString);
                ips.add(ip);
            }
            ipMap.put(varName, ips);
        }
        int recordNum = 0;

        EventSchema schema = EventSchema.getEventSchema(tableName);
        EventStore store = new EventStore(tableName + nodeId, false);
        long fileSize = store.getFileSize();

        long hasReadSize = 0;
        int curPage = 0;
        int curOffset = 0;
        int recordLen = schema.getFixedRecordLen();

        // read record in batch
        MappedByteBuffer pageBuffer = store.getMappedBuffer(curPage, fileSize);

        // due to we use fixed length record, we can obtain all record automatically
        while(hasReadSize < fileSize){
            if (curOffset + recordLen > EventStore.pageSize) {
                curPage++;
                hasReadSize += (EventStore.pageSize - curOffset);
                curOffset = 0;
                pageBuffer = store.getMappedBuffer(curPage, fileSize);
            }

            boolean noAdd = true;
            // check independent constraint list, note that ips can be null
            for(Map.Entry<String, List<IndependentPredicate>> entry : ipMap.entrySet()){
                String varName = entry.getKey();
                //IndependentPredicate
                List<IndependentPredicate> ips = entry.getValue();
                boolean satisfied = true;
                for(IndependentPredicate ip : ips){
                    String columnName = ip.getAttributeName();
                    Object obj = schema.getColumnValue(columnName, pageBuffer, curOffset);
                    if(!ip.check(obj, schema.getDataType(columnName))){
                        satisfied = false;
                        break;
                    }
                }

                if(satisfied){
                    if(noAdd){
                        byte[] record = new byte[recordLen];
                        pageBuffer.position(curOffset);
                        pageBuffer.get(record);
                        filteredRecords.add(record);
                        noAdd = false;
                        varPointers.get(varName).add(recordNum);
                    }else{
                        varPointers.get(varName).add(recordNum);
                    }
                }

            }
            if(!noAdd){
                recordNum++;
            }
            hasReadSize += recordLen;
            curOffset += recordLen;
        }

        //for(byte[] record : filteredRecords){
        //    System.out.println(schema.getRecordStr(record));
        //}

        //System.out.println("record number " + recordNum);

        return new EventCache(schema, filteredRecords, varPointers);
    }

    // please note that we require each variable's result cannot overlap
    public EventCache scanBasedVarName(Map<String, List<String>> ipStringMap){
        EventSchema schema = EventSchema.getEventSchema(tableName);
        EventStore store = new EventStore(tableName + nodeId, false);

        List<byte[]> filteredRecords = new ArrayList<>(1024);
        Map<String, List<Integer>> varPointers = new HashMap<>(ipStringMap.size() << 2);

        Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap = parseIpString(ipStringMap, schema);

        int recordNum = 0;
        long fileSize = store.getFileSize();
        int recordLen = schema.getFixedRecordLen();
        File file = store.getFile();
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            raf.seek(0);
            long bytesRead = 0;
            byte[] buffer = new byte[EventStore.pageSize];
            while (bytesRead < fileSize) {
                int read = raf.read(buffer);
                if (read == -1) break;
                bytesRead += read;
                // start process data
                int curRecordNum = read / recordLen;
                ByteBuffer bb = ByteBuffer.wrap(buffer);
                for(int i = 0; i < curRecordNum; i++){
                    for(Map.Entry<String, List<Pair<IndependentPredicate, ColumnInfo>>> entry : ipMap.entrySet()) {
                        String varName = entry.getKey();
                        List<Pair<IndependentPredicate, ColumnInfo>> ips = entry.getValue();
                        boolean satisfied = true;
                        for(Pair<IndependentPredicate, ColumnInfo> pair : ips){
                            IndependentPredicate ip = pair.getKey();
                            ColumnInfo columnInfo = pair.getValue();
                            Object obj = schema.getColumnValue(columnInfo, bb, i * recordLen);
                            //Object obj = schema.getColumnValue(columnName, bb, i * recordLen);
                            if(!ip.check(obj, columnInfo.getDataType())){
                                satisfied = false;
                                break;
                            }
                        }
                        if(satisfied){
                            byte[] record = new byte[recordLen];
                            System.arraycopy(buffer, i * recordLen, record, 0, recordLen);
                            filteredRecords.add(record);
                            varPointers.get(varName).add(recordNum);
                            recordNum++;
                            break;
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        //System.out.println("recordNum: " + recordNum);
        //for(byte[] record : filteredRecords){
        //    System.out.println(schema.getRecordStr(record));
        //}

        return new EventCache(schema, filteredRecords, varPointers);
    }
}
