package query;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * here we use binary tree to represent complex pattern
 * A complex query pattern statement:
 * -----------------------------------------------------------------
 * PATTERN SEQ(AND(ROBBERY v1, BATTERY v2), MOTOR_VEHICLE_THEFT v3)
 * USE SKIP-TILL-ANY-MATCH
 * WHERE v1.district = v2.district AND v2.district = v3.district
 * WITHIN 30 minutes
 * -----------------------------------------------------------------
 */
public class ComplexPattern extends PatternQuery {
    private PatternTree patternTree;

    public ComplexPattern(String firstLine) {
        super(firstLine);
        onlyContainSEQ = false;
        parse(firstLine);
    }

    @Override
    public void parse(String eventPatternStr) {
        // e.g. PATTERN SEQ(AND(ROBBERY v1, BATTERY v2), MOTOR_VEHICLE_THEFT v3)
        String patternStr = eventPatternStr.substring(7).trim();
        this.patternTree = new PatternTree(patternStr);
        // variable name -> event type
        this.varTypeMap = patternTree.getVarNameEventTypeMap();
    }

    @Override
    public boolean isOnlyLeftMostNode(String varName) {
        if(varTypeMap.containsKey(varName)){
            return patternTree.isLeftMostVariable(varName);
        }
        return false;
    }

    @Override
    public boolean isOnlyRightMostNode(String varName) {
        if(varTypeMap.containsKey(varName)){
            return patternTree.isRightMostVariable(varName);
        }
        return false;
    }

    @SuppressWarnings("unused")
    public List<PatternNode> getInorderTravel(){
        return patternTree.getInorderTraversal(patternTree.getRoot());
    }

    public void print(){
        // first line
        String patternStr = patternTree.print();
        System.out.println("PATTERN " + patternStr);

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

    public PatternOperator getProjectionOperator(String variableName1, String variableName2){
        List<PatternLeafNode> leafNodes = patternTree.getAllLeafNode();
        PatternNode node1 = null;
        PatternNode node2 = null;
        for(PatternLeafNode leftNode : leafNodes){
            if(leftNode.getVarName().equals(variableName1)){
                node1 = leftNode;
            }
            if(leftNode.getVarName().equals(variableName2)){
                node2 = leftNode;
            }
        }
        if(node1 == null || node2 == null){
            throw new RuntimeException("this query does not contain variable name: '" + variableName1 + "' & '" + variableName2 +"'");
        }
        Set<PatternNode> fathers = new HashSet<>();
        while(node1.getFather() != null){
            node1 = node1.getFather();
            fathers.add(node1);
        }
        while(node2.getFather() != null){
            node2 = node2.getFather();
            if(fathers.contains(node2)){
                break;
            }
        }
        PatternInternalNode internalNode = (PatternInternalNode) node2;
        return internalNode.getPatternOperator();
    }

    /*
    public static void main(String[] args){
        // String eventPattern = "PATTERN SEQ(AND(A v1, B v2), C v3)";
        String eventPattern = "PATTERN AND(AND(A v1, B v2), OR(AND(C v3, D v4), E v5))";
        // String eventPattern = "PATTERN AND(AND(A v1, B v2), OR(C v3, D v4))";
        ComplexPattern complexPattern = new ComplexPattern(eventPattern.toUpperCase());
        PatternOperator op = complexPattern.getProjectionOperator("V1", "V5");
        System.out.println("op: " + op);
    }
    */
}
