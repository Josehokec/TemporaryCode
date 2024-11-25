package query;

import java.util.*;

public abstract class PatternQuery {
    public boolean onlyContainSEQ;                                      // mark whether only contain SEQ operator
    protected String eventPattern;                                      // pattern
    protected int patternLen;                                           // a tuple that contain max event number
    protected int variableNum;                                          // number of variable
    protected long window;                                              // query time window
    protected SelectionStrategy strategy;                               // strategy
    protected HashMap<String, List<IndependentPredicate>> ipMap;        // independent predicate list
    protected List<DependentPredicate> dpList;                         // dependent constraint list
    protected Map<String, String> varTypeMap;                           // variable name -> event type

    public PatternQuery(String firstLine){
        this.eventPattern = firstLine;
        // parse use reference, so we need to initialize
        ipMap = new HashMap<>();
        dpList = new ArrayList<>();
        varTypeMap = new HashMap<>();
    }

    // below we define abstract methods
    /**
     * query statement first line example 1:
     * PATTERN SEQ(ROBBERY v0, BATTERY v1, MOTOR_VEHICLE_THEFT v2)
     * query statement first line example 2:
     * PATTERN SEQ(AND(ROBBERY v0, BATTERY v1), MOTOR_VEHICLE_THEFT v2)
     * @param firstLine query statement firstLine
     */
    public abstract void parse(String firstLine);
    public abstract boolean isOnlyLeftMostNode(String varName);
    public abstract boolean isOnlyRightMostNode(String varName);
    public abstract PatternOperator getProjectionOperator(String variableName1, String variableName2);
    public abstract void print();

    public Set<String> getQueriedEventType(){
        Set<String> ans = new HashSet<>();
        for(String type: varTypeMap.values()){
            ans.add(type);
        }
        return ans;
    }

    public List<DependentPredicate> getDpList() {
        return dpList;
    }

    @SuppressWarnings("unused")
    public int getVariableNum(){ return variableNum; }

    @SuppressWarnings("unused")
    public String getEventType(String varName){
        return varTypeMap.get(varName);
    }

    public List<IndependentPredicate> getIndependentPredicateList(String variableName){
        if(ipMap.containsKey(variableName)){
            return ipMap.get(variableName);
        }else{
            return new ArrayList<>();
        }

    }

    public void setQueryWindow(long window){
        this.window = window;
    }

    public long getQueryWindow(){
        return window;
    }

    public void setSelectionStrategy(SelectionStrategy strategy){
        this.strategy = strategy;
    }

    public void insertIndependentPredicate(IndependentPredicate ip){
        String variableName = ip.getVariableName();
        if(ipMap.containsKey(variableName)){
            ipMap.get(variableName).add(ip);
        }else{
            List<IndependentPredicate> ipList = new ArrayList<>(8);
            ipList.add(ip);
            ipMap.put(variableName, ipList);
        }
    }

    public void insertDependentPredicate(DependentPredicate dp){
        dpList.add(dp);
    }

    public SelectionStrategy getStrategy(){
        return strategy;
    }

    public boolean existOROperator(){
        return eventPattern.contains("OR(");
    }

    public Map<String, String> getVarTypeMap() {
        return varTypeMap;
    }

    public List<DependentPredicate> getRelatedDependentPredicate(Set<String> preVarNames, String curVarName){
        List<DependentPredicate> ans = new ArrayList<>();
        for(DependentPredicate dp : dpList){
            String varName1= dp.getLeftVariableName();
            String varName2 = dp.getRightVariableName();
            if(curVarName.equals(varName1) && preVarNames.contains(varName2)){
                ans.add(dp);
            }else if(curVarName.equals(varName2) && preVarNames.contains(varName1)){
                ans.add(dp);
            }
        }
        return ans;
    }

    public List<DependentPredicate> getRelatedDependentPredicate(String variableName1, String variableName2){
        List<DependentPredicate> ans = new ArrayList<>();
        for(DependentPredicate dp : dpList){
            String varName1= dp.getLeftVariableName();
            String varName2 = dp.getRightVariableName();
            if(variableName1.equals(varName1) && variableName2.equals(varName2)){
                ans.add(dp);
            }else if(variableName2.equals(varName2) && variableName1.equals(varName1)){
                ans.add(dp);
            }
        }
        return ans;
    }

    public List<EqualDependentPredicate> getRelatedEqualDependentPredicate(String variableName1, String variableName2){
        List<EqualDependentPredicate> ans = new ArrayList<>();
        for(DependentPredicate dp : dpList){
            if(dp.isEqualDependentPredicate()){
                String varName1= dp.getLeftVariableName();
                String varName2 = dp.getRightVariableName();
                if(variableName1.equals(varName1) && variableName2.equals(varName2)){
                    ans.add((EqualDependentPredicate) dp);
                }else if(variableName2.equals(varName2) && variableName1.equals(varName1)){
                    ans.add((EqualDependentPredicate) dp);
                }
            }
        }
        return ans;
    }

    @Deprecated
    public void getAllVarType(String[] varNames, String[] eventTypes){
        if(varNames.length != varTypeMap.size()){
            System.out.println("varNames size: " + varNames.length + ", map size: " + varTypeMap.size());
            throw new RuntimeException("length mismatch");
        }
        int cnt = 0;
        for(Map.Entry<String, String> entry : varTypeMap.entrySet()){
            varNames[cnt] = entry.getKey();
            eventTypes[cnt] = entry.getValue();
            cnt++;
        }
    }

    public String getEventPattern() { return eventPattern; }

    public HashMap<String, List<IndependentPredicate>> getIpMap() {
        return ipMap;
    }
}