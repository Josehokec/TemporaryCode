package query;


import event.DataType;
import event.Event;

/**
 * Independent predicate constraints
 * supported format: variableName.attributeName ComparedOperator constantValue
 * cannot supported format: variableName.attributeName BETWEEN constantValue1 AND constantValue2
 */


public class IndependentPredicate {
    String variableName;
    String attributeName;
    ComparedOperator operator;
    String constantValue;

    IndependentPredicate(String singlePredicate){
        // v1.beat <= 1
        String str = singlePredicate.trim();
        int len = str.length();
        int dotPos = -1;
        for(int i = 0; i < len; ++i){
            if(str.charAt(i) == '.'){
                dotPos = i;
                variableName = str.substring(0, i);
            }
            if(dotPos != -1){
                // GT: '>'  LT: '<' GE: '>='
                // LE: '<=' EQ: '=' NEQ: '!='
                char ch = str.charAt(i);
                if(ch == '>' || ch == '<' || ch == '=' || ch == '!'){
                    attributeName = str.substring(dotPos + 1, i).trim();
                    char nextChar = str.charAt(i + 1);
                    switch (ch){
                        case '>':
                            if(nextChar == '='){
                                operator = ComparedOperator.GE;
                                constantValue = str.substring(i + 2);
                            }else{
                                operator = ComparedOperator.GT;
                                constantValue = str.substring(i + 1);
                            }
                            break;
                        case '<':
                            if(nextChar == '='){
                                operator = ComparedOperator.LE;
                                constantValue = str.substring(i + 2);
                            }else{
                                operator = ComparedOperator.LT;
                                constantValue = str.substring(i + 1);
                            }
                            break;
                        case '=':
                            operator = ComparedOperator.EQ;
                            constantValue = str.substring(i + 1);
                            break;
                        case '!':
                            operator = ComparedOperator.NEQ;
                            constantValue = str.substring(i + 2);
                    }
                    constantValue = constantValue.trim();
                    // skip loop
                    break;
                }
            }
        }
    }

    public boolean check(Object obj, DataType dataType){
        // to get more fast process performance, we choose Object rather than Event
        if(dataType != DataType.STRING){
            double leftValue = 0;
            double rightValue = 0;
            switch (dataType){
                case INT:
                    leftValue = (int) obj;
                    rightValue = Integer.parseInt(constantValue);
                    break;
                case FLOAT:
                    leftValue = (float) obj;
                    rightValue = Float.parseFloat(constantValue);
                    break;
                case LONG:
                    leftValue = (long) obj;
                    rightValue = Long.parseLong(constantValue);
                    break;
                case DOUBLE:
                    leftValue = (double) obj;
                    rightValue = Double.parseDouble(constantValue);
            }
            switch (operator){
                case NEQ:
                    return leftValue != rightValue;
                case LT:
                    return leftValue < rightValue;
                case LE:
                    return leftValue <= rightValue;
                case GT:
                    return leftValue > rightValue;
                case GE:
                    return leftValue >= rightValue;
                case EQ:
                    return leftValue == rightValue;
            }
        }else{
            // String type
            String leftValue = "'" + ((String) obj).toUpperCase() + "'";
            switch (operator){
                case NEQ:
                    return !leftValue.equals(constantValue);
                case LT:
                    return leftValue.compareTo(constantValue) < 0;
                case LE:
                    return leftValue.compareTo(constantValue) <= 0;
                case GT:
                    return leftValue.compareTo(constantValue) > 0;
                case GE:
                    return leftValue.compareTo(constantValue) >= 0;
                case EQ:
                    return leftValue.equals(constantValue);
            }
        }
        return false;
    }

    public boolean check(Event event){
        Object obj =  event.getAttributeValue(attributeName);
        DataType dataType = event.getDataType(attributeName);
        return check(obj, dataType);
    }

    public String getVariableName(){
        return variableName;
    }

    public String getAttributeName(){
        return attributeName;
    }

    public void print(){
        System.out.print(variableName + "." + attributeName);
        switch (operator){
            case EQ:
                System.out.print(" = ");
                break;
            case GE:
                System.out.print(" >= ");
                break;
            case GT:
                System.out.print(" > ");
                break;
            case LE:
                System.out.print(" <= ");
                break;
            case LT:
                System.out.print(" < ");
                break;
            default:
                // NEQ
                System.out.print(" != ");
        }
        System.out.println(constantValue);
    }

    public static void main(String[] args){
        String predicate1 = "v1.beat = 5";
        String predicate2 = "v1.beat >= 5";
        String predicate3 = "v1.beat != 5.0";
        String predicate4 = "v1.beat < 5.0";
        String predicate5 = "v1.beat != X";
        String predicate6 = "v1.beat = X";

        IndependentPredicate ip1 = new IndependentPredicate(predicate1);
        ip1.print();
        System.out.println("true: "  + ip1.check(5, DataType.INT));
        System.out.println("false: "  + ip1.check(6, DataType.INT));

        IndependentPredicate ip2 = new IndependentPredicate(predicate2);
        ip2.print();
        System.out.println("false: "  + ip2.check(4, DataType.INT));
        System.out.println("true: "  + ip2.check(5L, DataType.LONG));
        System.out.println("true: "  + ip2.check(6.0, DataType.DOUBLE));

        IndependentPredicate ip3 = new IndependentPredicate(predicate3);
        ip3.print();
        System.out.println("true: "  + ip3.check(5.5f, DataType.FLOAT));
        System.out.println("false: "  + ip3.check(5.0f, DataType.FLOAT));

        IndependentPredicate ip4 = new IndependentPredicate(predicate4);
        ip4.print();
        System.out.println("true: "  + ip4.check(4.0, DataType.DOUBLE));
        System.out.println("false: "  + ip4.check(5.0, DataType.DOUBLE));

        IndependentPredicate ip5 = new IndependentPredicate(predicate5);
        ip5.print();
        System.out.println("true: "  + ip5.check("Y", DataType.STRING));
        System.out.println("false: "  + ip5.check("X", DataType.STRING));

        IndependentPredicate ip6 = new IndependentPredicate(predicate6);
        ip6.print();
        System.out.println("false: "  + ip6.check("Y", DataType.STRING));
        System.out.println("true: "  + ip6.check("X", DataType.STRING));
    }

}
