package query;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 查询解析器；
 * 我们把查询分为四个部分
 * 事件模式，匹配策略，谓词约束，时间窗口
 * PATTERN SEQ(ROBBERY v1, BATTERY v2, MOTOR_VEHICLE_THEFT v3)
 * USING SKIP_TILL_ANY_MATCH
 * WHERE v1.district >= 10 AND v1.district <= 20 AND v1.District = v2.District AND v1.District = v3.District
 * WITHIN 30 minutes
 */
public class QueryParse {
    public static PatternQuery parseQueryString(String queryStr){
        PatternQuery patternQuery;

        String str = queryStr.toUpperCase();
        String[] fourParts = str.split("\n");
        // first line
        String eventPatternStr = fourParts[0];
        if(eventPatternStr.substring(12).contains("(")){
            // complex event pattern
            patternQuery = new ComplexPattern(eventPatternStr);
        }else{
            // sequential event pattern
            patternQuery = new SequentialPattern(eventPatternStr);
        }

        // second line
        String selectionStrategyStr = fourParts[1].substring(6);
        SelectionStrategy strategy;
        if(selectionStrategyStr.equals("SKIP_TILL_ANY_MATCH")){
            strategy = SelectionStrategy.SKIP_TILL_ANY_MATCH;
        }else if(selectionStrategyStr.equals("SKIP_TILL_NEXT_MATCH")){
            strategy = SelectionStrategy.SKIP_TILL_NEXT_MATCH;
        }else if(selectionStrategyStr.equals("STRICT_CONTIGUOUS")){
            strategy = SelectionStrategy.STRICT_CONTIGUOUS;
        }else{
            System.out.println("'" + selectionStrategyStr + "' selection strategy does not define, we set SKIP_TILL_ANY_MATCH");
            strategy = SelectionStrategy.SKIP_TILL_ANY_MATCH;
        }
        patternQuery.setSelectionStrategy(strategy);

        // third line
        String predicates = fourParts[2].substring(6);
        String[] predicateList =  predicates.split("AND");
        // old version: [A-Za-z0-9]+[.][A-Za-z0-9]+[\s]*([<>!=][=]?)[\s]*(([0-9]+(.[0-9]+)?)|([A-Za-z0-9]+))
        String independentPredicateRegex = "[A-Za-z0-9]+[.][A-Za-z0-9]+[\\s]*([<>!=][=]?)[\\s]*[']?(([0-9]+(.[0-9]+)?)|([A-Za-z0-9]+))[']?";
        Pattern pattern = Pattern.compile(independentPredicateRegex);
        for(int i = 0; i < predicateList.length; ++i){
            String singlePredicate = predicateList[i].trim();
            Matcher matcher = pattern.matcher(singlePredicate);
            if(matcher.matches()){
                IndependentPredicate ip = new IndependentPredicate(singlePredicate);
                patternQuery.insertIndependentPredicate(ip);
                // System.out.print("read: "); ip.print();
            }else{
                // judge equal predicate or non-equal predicate
                int pos = singlePredicate.indexOf('=');
                DependentPredicate dp;
                if(pos == -1){
                    dp = new NoEqualDependentPredicate(singlePredicate);
                }else{
                    char preChar = singlePredicate.charAt(pos - 1);
                    if(preChar != '!' && preChar != '<' && preChar != '>'){
                        dp = new EqualDependentPredicate(singlePredicate);
                    }else{
                        dp = new NoEqualDependentPredicate(singlePredicate);
                    }
                }
                patternQuery.insertDependentPredicate(dp);
            }
        }

        // WITHIN 30 minutes
        String[] windowStringList = fourParts[3].split(" ");
        long value = Long.parseLong(windowStringList[1]);
        long queryWindow;
        // By default, we use microseconds as a time unit
        String timeUnit = windowStringList[2];
        if(timeUnit.contains("HOUR")){
            queryWindow = value * 60 * 60 * 1000;
        }else if(timeUnit.contains("MIN")){
            queryWindow = value * 60 * 1000;
        }else if(timeUnit.contains("MICROSECOND")){
            queryWindow = value;
        }else if(timeUnit.contains("SEC")){
            queryWindow = value * 1000;
        }else{
            queryWindow = value;
        }
        patternQuery.setQueryWindow(queryWindow);

        return patternQuery;
    }
}
