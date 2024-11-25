package rpc;

import query.DependentPredicate;
import store.DataType;
import store.EventSchema;
import utils.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * this class is used for pull-push based methods join
 * it can use equal predicates and non-equal predicates
 */

public class JoinFilter4TwoVars {
    private final String hasVisitedVarName;
    private List<Pair<Long, byte[]>> entireVarEvents;
    private final String curVarName;
    private List<Pair<Long, byte[]>> partialVarEvents;
    private final List<DependentPredicate> dpList;
    private final boolean previousOrNext;
    private final EventSchema schema;
    private final long window;

    public JoinFilter4TwoVars(String hasVisitedVarName, List<byte[]> entireEventList, String curVarName, List<byte[]> partialEventList, List<DependentPredicate> dpList, EventSchema schema, boolean previousOrNext, long window) {
        this.hasVisitedVarName = hasVisitedVarName;
        this.curVarName = curVarName;
        this.dpList = dpList;
        this.previousOrNext = previousOrNext;
        this.window = window;
        this.schema = schema;
        entireVarEvents = new ArrayList<>(entireEventList.size());

        for(byte[] record : entireEventList) {
            long ts = schema.getTimestamp(record);
            entireVarEvents.add(new Pair<>(ts, record));
        }
        entireVarEvents.sort(Comparator.comparingLong(Pair::getKey));

        partialVarEvents = new ArrayList<>(partialEventList.size());
        for(byte[] record : partialEventList) {
            long ts = schema.getTimestamp(record);
            partialVarEvents.add(new Pair<>(ts, record));
        }
        partialVarEvents.sort(Comparator.comparingLong(Pair::getKey));
    }

    public List<Integer> join(){
        // false (previous): SEQ(hasVisitedVarName, curVarName), true: SEQ(curVarName, hasVisitedVarName)
        if(previousOrNext) {
            return reverseSeqJoin();
        }else{
            return seqJoin();
        }

    }

    public List<Integer> seqJoin(){
        List<Integer> pointers = new ArrayList<>();
        int idx = 0;
        int size = partialVarEvents.size();
        for(Pair<Long, byte[]> entry : entireVarEvents) {
            long leftTs = entry.getKey();
            byte[] leftRecord = entry.getValue();

            for(int i = idx; i < size; i++){
                Pair<Long, byte[]> rightPair = partialVarEvents.get(i);
                long rightTs = rightPair.getKey();
                if(leftTs > rightTs){
                    idx = i;
                }else if(rightTs - leftTs > window){
                    break;
                }else{
                    // check dp
                    byte[] rightRecord = rightPair.getValue();
                    boolean satisfy = true;
                    for(DependentPredicate dp : dpList){
                        String colName = dp.getAttributeName();
                        Object leftObj = schema.getColumnValue(colName, leftRecord);
                        Object rightObj = schema.getColumnValue(colName, rightRecord);
                        DataType dateType = schema.getDataType(colName);
                        if(!dp.check(hasVisitedVarName, leftObj, curVarName, rightObj, dateType)){
                            satisfy = false;
                            break;
                        }
                    }
                    if(satisfy){
                        pointers.add(i);
                        idx = i;
                    }
                }
            }
        }
        return pointers;
    }

    public List<Integer> reverseSeqJoin(){
        List<Integer> pointers = new ArrayList<>();
        int idx = 0;
        int leftSize = partialVarEvents.size();
        int rightSize = entireVarEvents.size();
        for(int pointer = 0; pointer < leftSize; pointer++) {
            Pair<Long, byte[]> entry = partialVarEvents.get(pointer);
            long leftTs = entry.getKey();
            byte[] leftRecord = entry.getValue();

            for(int i = idx; i < rightSize; i++){
                Pair<Long, byte[]> rightPair = partialVarEvents.get(i);
                long rightTs = rightPair.getKey();
                if(leftTs > rightTs){
                    idx = i;
                }else if(rightTs - leftTs > window){
                    break;
                }else{
                    byte[] rightRecord = rightPair.getValue();
                    boolean satisfy = true;
                    for(DependentPredicate dp : dpList){
                        String colName = dp.getAttributeName();
                        Object leftObj = schema.getColumnValue(colName, leftRecord);
                        Object rightObj = schema.getColumnValue(colName, rightRecord);
                        DataType dateType = schema.getDataType(colName);
                        if(!dp.check(hasVisitedVarName, leftObj, curVarName, rightObj, dateType)){
                            satisfy = false;
                            break;
                        }
                    }
                    if(satisfy){
                        pointers.add(leftSize);
                    }
                }
            }
        }

        return pointers;
    }
}
