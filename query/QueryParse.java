package query;

import event.TimeGranularity;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * here we only parse the query semantic that flink sql support
 * details please see:
 * <a href="https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/queries/match_recognize/#known-limitations"></a>
 * besides, we require
 * (1) the match_recognize sql contains the window condition;
 * (2) follow by vldb23 high performance row pattern recognize using joins, i.e.,
 * PATTERN FORMAT: (variableName, irrelevantVariableName*?, variableName, irrelevantVariableName*?, ...),
 * which means we allow skip irrelevant events between two variableName
 * two reasons: distributed, event arrival may unordered
 */
public class QueryParse {
    private boolean unsupportedQuery = false;
    private final String tableName;
    private final List<String> variableNames;
    private long window;
    private TimeGranularity timeGranularity;
    // IndependentPredicate: variableName.attributeName < 10
    private final Map<String, List<String>> ipStringMap = new HashMap<>();
    // DependentPredicate: variableName1.attributeName = variableName2.attributeName
    private final List<String> dpStringList = new ArrayList<>();

    /**
     * obtain tableName, pattern, and predicate constraints
     * we do not process SUM and LAST keywords
     * @param sql - a sql contains keyword `MATCH_RECOGNIZE`
     */
    public QueryParse(String sql){
        sql = sql.toUpperCase().replace("\n", "").replace("\r", "");
        Pattern tableNamePattern = Pattern.compile("SELECT\\s+[*]\\s+FROM\\s+(\\w+)\\s*MATCH_RECOGNIZE");
        Matcher tableNameMatcher = tableNamePattern.matcher(sql);
        if (tableNameMatcher.find()) {
            tableName = tableNameMatcher.group(1);
        }else{
            throw new RuntimeException("Wrong sql statement (cannot identify table name), sql: " + sql);
        }

        int patternClausePos = sql.indexOf("PATTERN");
        int withinClausePos = sql.indexOf("WITHIN");
        int defineClausePos = sql.indexOf("DEFINE");

        if(patternClausePos == -1 || withinClausePos == -1){
            throw new RuntimeException("Wrong sql statement (without keywords 'PATTERN' or 'WITHIN'), sql: " + sql);
        }

        // please note that we do not process LAST and SUM keywords
        // besides, currently, we do not support OR and BETWEEN keywords
        if(defineClausePos != -1){
            int len = sql.length();
            int defineClauseEnd = sql.length();
            for(int i = len - 1; i > 0; --i){
                char ch = sql.charAt(i);
                if(ch == ')'){
                    defineClauseEnd = i;
                    break;
                }
            }
            // 'DEFINE' has 7 characters
            String defineClauses = sql.substring(defineClausePos + 6, defineClauseEnd);
            String[] defineForVariables = defineClauses.split(",");
            for(String defineForSingleVar : defineForVariables){
                String[] leftAndRight = defineForSingleVar.split(" AS ");
                String varName = leftAndRight[0].trim();

                ipStringMap.put(varName, new ArrayList<>(4));

                if(leftAndRight[1].contains(" BETWEEN ")){
                    // variableName.attributeName BETWEEN a AND b
                    throw new RuntimeException("Currently we cannot support 'A between x and y' syntax, you can change it as 'A >= x AND A <= y'");
                }else{
                    String[] predicates = leftAndRight[1].trim().split(" AND ");
                    // variableName.attributeName [<>!=][=]? value
                    String ipRegex = "[A-Za-z_][A-Za-z0-8_]*[.][A-Za-z_][A-Za-z0-8_]*[\\s+]*([<>!=]=?)\\s*'?(([0-9]+(.[0-9]+)?)|([A-Za-z0-9_]+))'?";
                    Pattern pattern = Pattern.compile(ipRegex);
                    for(String predicate : predicates){
                        if(!predicate.contains("SUM") && !predicate.contains("LAST")){
                            Matcher matcher = pattern.matcher(predicate.trim());
                            if(matcher.matches()) {
                                // IndependentPredicate ip = new IndependentPredicate(predicate);
                                // ipMap.get(varName).add(ip);
                                ipStringMap.get(varName).add(predicate);
                            }else {
//                                int pos = predicate.indexOf('=');
//                                DependentPredicate dp;
//                                if(pos == -1){
//                                    dp = new NoEqualDependentPredicate(predicate);
//                                }else{
//                                    char preChar = predicate.charAt(pos - 1);
//                                    if(preChar != '!' && preChar != '<' && preChar != '>'){
//                                        dp = new EqualDependentPredicate(predicate);
//                                    }else{
//                                        dp = new NoEqualDependentPredicate(predicate);
//                                    }
//                                }
                                dpStringList.add(predicate);
                            }
                        }
                    }
                }
            }
        }

        // PATTERN -> 7 characters
        String patternClause = sql.substring(patternClausePos + 7, withinClausePos).trim();

        // pattern always (variableName[+*][?]?)
        String[] nameArray = patternClause.substring(1, patternClause.length() - 1).split("\\s+");
        int variableNameNum = nameArray.length;
        variableNames = new ArrayList<>((nameArray.length + 1) >> 1);
        // we require even
        if((variableNameNum & 0x01) != 1){
            unsupportedQuery = true;
            return;
        }else{
            for (int i = 0; i < variableNameNum; i++) {
                String name = nameArray[i];
                int strLen = name.length();
                if((i & 0x01) == 1){
                    if(!name.contains("*?") || ipStringMap.containsKey(name.substring(0, strLen - 2))){
                        unsupportedQuery = true;
                        return;
                    }
                }else{
                    // we allow this variable contain {n,m} or +?
                    for(int j = 0; j < strLen; ++j){
                        char ch = name.charAt(j);
                        if(ch == '+' || ch == '{' || ch == '*'){
                            unsupportedQuery = true;
                            return;
                        }
                    }
                    if(!ipStringMap.containsKey(name)){
                        unsupportedQuery = true;
                        return;
                    }
                    variableNames.add(name);
                }
            }
        }

        // parse window
        String[] splits = sql.substring(withinClausePos, defineClausePos).split("\\s+");
        window = Long.parseLong(splits[2].substring(1, splits[2].length() - 1));
        if(splits[3].contains("HOUR")){
            timeGranularity = TimeGranularity.HOUR;
        }else if(splits[3].contains("MINUTE")){
            timeGranularity = TimeGranularity.MINUTER;
        }else if(splits[3].contains("MILLISECOND")){
            timeGranularity = TimeGranularity.SECOND;
        }else if(splits[3].contains("SECOND")){
            timeGranularity = TimeGranularity.MILLISECOND;
        }else{
            timeGranularity = TimeGranularity.SECOND;
        }
    }

    public void print(){
        if(unsupportedQuery){
            System.out.println("we cannot support this query, we more care about allow skip semantic.");
        }else{
            System.out.println("table names: " + tableName);
            System.out.println("variable names: " + variableNames);
            System.out.println("window: " + window + " " + timeGranularity);
            // here we do not process SUM and LAST keywords
            ipStringMap.forEach((k, v) -> System.out.print("variable name: " + k + " ip list: " + v +" "));
            System.out.println("\ndp list: " + dpStringList);
        }
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getVariableNames() {
        return variableNames;
    }

    // please note that we assume that the time granularity of stored event is s
    public long getWindow() {
        switch (timeGranularity) {
            case HOUR:
                return window * 3600;
            case MINUTER:
                return window * 60;
            case SECOND:
                return window;
            case MILLISECOND:
                return window / 1000;
            default:
                System.out.println("please note that this query do not give time granularity, we set SECOND");
                return window;
        }
    }

    public TimeGranularity getTimeGranularity() {
        return timeGranularity;
    }

    /**
     * 0: head variable, 1: tail variable, 2: middle variable
     * @param variableName variable name
     * @return  head or tail variable marker
     */
    public int headTailMarker(String variableName){
        String firstVariableName = variableNames.get(0);
        if(firstVariableName.equals(variableName)){
            return 0;
        }
        String lastVariableName = variableNames.get(variableNames.size() - 1);
        if(lastVariableName.equals(variableName)){
            return 1;
        }
        return 2;
    }

    public String getHeadVarName(){
        return variableNames.get(0);
    }

    public String getTailVarName(){
        return variableNames.get(variableNames.size() - 1);
    }

    public Map<String, List<String>> getIpStringMap() {
        return ipStringMap;
    }

    public List<String> getDpStringList() {
        return dpStringList;
    }

    public boolean compareSequence(String hasVisitVarName, String varName){
        // false: previous, true: next
        for(String str : variableNames){
            if(str.equals(hasVisitVarName)){
                return false;
            }
            if(str.equals(varName)){
                return true;
            }
        }
        System.out.println("wrong state, maybe has wrong variable name");
        return false;
    }

    /**
     * note that users may give: V1.BEAT = V2.BEAT + 1 AND V2.DISTRICT = V1.DISTRICT
     * to avoid generate two keys, we define key = varName_{x}-varName_{y}, where x < y
     * @return varName_{x}-varName_{y}, predicates
     */
    public Map<String, List<String>> getDpMap(){
        Map<String, List<String>> dpMap = new HashMap<>(8);
        for(String dpStr : dpStringList){
            DependentPredicate dp = new EqualDependentPredicate(dpStr);
            String varName1 = dp.getLeftVariableName();
            String varName2 = dp.getRightVariableName();
            String key = varName1.compareTo(varName2) < 0 ? (varName1 + "-" + varName2) : (varName2 + "-" + varName1);
            if(dpMap.containsKey(key)){
                dpMap.get(key).add(dpStr);
            } else{
                List<String> predicates = new ArrayList<>(4);
                predicates.add(dpStr);
                dpMap.put(key, predicates);
            }
        }
        return dpMap;
    }

    public boolean isUnsupportedQuery() {
        return unsupportedQuery;
    }

    public static void main(String[] args){
        String querySQL =
                "SELECT * FROM Crimes MATCH_RECOGNIZE(\n" +
                        "    ORDER BY timestamp\n" +
                        "    MEASURES A.id as AID, B.id as BID, C.id AS CID\n" +
                        "    ONE ROW PER MATCH\n" +
                        "    AFTER MATCH SKIP TO NEXT ROW \n" +
                        "    PATTERN (A N1*? B N2*? C N3*? D) WITHIN INTERVAL '30' MINUTER\n" +
                        "    DEFINE \n" +
                        "        A AS A.primary_type = 'ROBBERY' AND A.beat >= 1900 AND B.beat <= 2000, \n" +
                        "        B AS B.primary_type = 'BATTERY', \n" +
                        "        C AS C.primary_type = 'MOTOR_VEHICLE_THEFT', \n" +
                        "        D AS D.BEAT = A.BEAT\n" +
                        ") MR;";
        QueryParse parser = new QueryParse(querySQL);
        System.out.println(parser.ipStringMap);
    }
}
