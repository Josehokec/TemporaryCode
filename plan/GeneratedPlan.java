package plan;

import org.apache.flink.table.planner.expressions.In;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GeneratedPlan {
    public static Plan basicPlan(Map<String, Integer> varEventNumMap, String headVarName, String tailVarName){
        int varNum = varEventNumMap.size();
        Map<String, Integer> weightMap = new HashMap<>(varNum << 1);
        for(Map.Entry<String, Integer> entry : varEventNumMap.entrySet()){
            String key = entry.getKey();
            int value = entry.getValue();
            if(key.equals(headVarName) || key.equals(tailVarName)){
                weightMap.put(key, value >> 1);
            }else{
                weightMap.put(key, value);
            }
        }

        List<Map.Entry<String, Integer>> sortedEntries = new ArrayList<>(weightMap.entrySet());
        sortedEntries.sort(Map.Entry.comparingByValue());

        Plan plan = new Plan(varNum);
        for(Map.Entry<String, Integer> entry : sortedEntries){
            plan.add(entry.getKey());
        }
        return plan;
    }
}
