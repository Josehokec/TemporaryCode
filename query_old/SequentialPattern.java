package query;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SequentialPattern extends PatternQuery {
    String[] seqEventTypes;
    String[] seqVarNames;
    private HashMap<String, Integer> varPosMap;	// accelerate find position

    public SequentialPattern(String firstLine){
        super(firstLine);
        onlyContainSEQ = true;
        varPosMap = new HashMap<>();
        parse(firstLine);
    }

    @Override
    public void parse(String firstLine) {
        // e.g., PATTERN SEQ(ROBBERY v0, BATTERY v1, MOTOR_VEHICLE_THEFT v2)
        String[] seqStatement = firstLine.split("[()]");
        // seqEvent = "ROBBERY v0, BATTERY v1, MOTOR_VEHICLE_THEFT v2"
        String[] seqEvent = seqStatement[1].split(",");
        this.patternLen = seqEvent.length;
        this.variableNum = patternLen;
        this.seqEventTypes = new String[patternLen];
        this.seqVarNames = new String[patternLen];

        for(int i = 0; i < patternLen; ++i){
            String[] s = seqEvent[i].trim().split(" ");
            seqEventTypes[i] = s[0];
            seqVarNames[i] = s[1].trim();
            // update two maps
            varTypeMap.put(seqVarNames[i], seqEventTypes[i]);
            varPosMap.put(seqVarNames[i], i);
        }
    }

    @Override
    public boolean isOnlyLeftMostNode(String varName) {
        return varPosMap.get(varName) == 0;
    }

    @Override
    public boolean isOnlyRightMostNode(String varName) {
        return varPosMap.get(varName) == patternLen - 1;
    }

    @Override
    public PatternOperator getProjectionOperator(String variableName1, String variableName2){
        return PatternOperator.SEQ;
    }

    @Override
    public void print() {
        // first line
        int len = seqEventTypes.length;
        StringBuffer buff = new StringBuffer(256);
        buff.append("PATTERN SEQ(");
        for(int i = 0; i < len; ++i){
            buff.append(seqEventTypes[i]).append(" ").append(seqVarNames[i]);
            if(i == len -1){
                buff.append(")");
            }else{
                buff.append(", ");
            }
        }
        System.out.println(buff);

        // second line
        switch (strategy){
            case STRICT_CONTIGUOUS:
                System.out.println("USING STRICT_CONTIGUOUS");
                break;
            case SKIP_TILL_NEXT_MATCH:
                System.out.println("USING SKIP_TILL_NEXT_MATCH");
                break;
            default:
                System.out.println("USING SKIP_TILL_ANY_MATCH");
        }

        // third line
        System.out.print("WHERE ");
        boolean addAndStr = false;
        for(List<IndependentPredicate> valueList: ipMap.values()){
            for(int i = 0; i < valueList.size(); ++i){
                if(addAndStr){
                    System.out.print("  AND ");
                }
                valueList.get(i).print();
                addAndStr = true;
            }
        }
        for(int i = 0; i < dpList.size(); ++i){
            if(addAndStr){
                System.out.print("  AND ");
            }
            dpList.get(i).print();
            addAndStr = true;
        }

        // fourth line
        System.out.println("WITHIN " + window + " UNITS/MICROSECONDS");
    }
}
