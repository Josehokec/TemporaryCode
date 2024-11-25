package query;

import store.DataType;
import event.Event;

import java.util.ArrayList;
import java.util.List;

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
    // this variable help us to accelerate get value
    double rightValue;
    boolean hasConvert;

    public IndependentPredicate(String singlePredicate){
        hasConvert = false;
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
                                constantValue = str.substring(i + 2).trim();
                            }else{
                                operator = ComparedOperator.GT;
                                constantValue = str.substring(i + 1).trim();
                            }
                            break;
                        case '<':
                            if(nextChar == '='){
                                operator = ComparedOperator.LE;
                                constantValue = str.substring(i + 2).trim();
                            }else{
                                operator = ComparedOperator.LT;
                                constantValue = str.substring(i + 1).trim();
                            }
                            break;
                        case '=':
                            operator = ComparedOperator.EQ;
                            constantValue = str.substring(i + 1).trim();
                            break;
                        case '!':
                            operator = ComparedOperator.NEQ;
                            constantValue = str.substring(i + 2).trim();
                    }
                    // skip loop
                    break;
                }
            }
        }
    }

    public boolean check(Object obj, DataType dataType){
        // to get more fast process performance, we first get its double value
        if(dataType != DataType.VARCHAR){
            if(!hasConvert){
                switch (dataType){
                    case INT:
                        rightValue = Integer.parseInt(constantValue);
                        break;
                    case FLOAT:
                        rightValue = Float.parseFloat(constantValue);
                        break;
                    case LONG:
                        rightValue = Long.parseLong(constantValue);
                        break;
                    default: // case DOUBLE:
                        rightValue = Double.parseDouble(constantValue);
                }
                hasConvert = true;
            }

            double leftValue;
            switch (dataType){
                case INT:
                    leftValue = (int) obj;
                    break;
                case FLOAT:
                    leftValue = (float) obj;
                    break;
                case LONG:
                    leftValue = (long) obj;
                    break;
                default:    //case DOUBLE:
                    leftValue = (double) obj;
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
            // String type only support equal operation
            // here we use below codes to accelerate
            byte[] leftValue = (byte[]) obj;
            int len = constantValue.length();
            for(int i = 1; i < len - 1; i++){
                // constantValue format: 'XXXXX'
                if(constantValue.charAt(i) != leftValue[i - 1]){
                    return false;
                }
            }
            return leftValue[len - 1] == 0;
        }
        return false;
    }

    public String getVariableName(){
        return variableName;
    }

    public String getAttributeName(){
        return attributeName;
    }

    // version 11-23: batch process
    //public void batchCheck(byte[] content, int readSize, int recordLen, )

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

    @Override
    public String toString(){
        StringBuilder str = new StringBuilder(128);
        str.append(variableName).append(".").append(attributeName);
        switch (operator){
            case EQ:
                str.append(" = ");
                break;
            case GE:
                str.append(" >= ");
                break;
            case GT:
                str.append(" > ");
                break;
            case LE:
                str.append(" <= ");
                break;
            case LT:
                str.append(" < ");
                break;
            default:
                // NEQ
                str.append(" != ");
        }
        str.append(constantValue);
        return str.toString();
    }
}


/*
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
        System.out.println("true: "  + ip5.check("Y", DataType.VARCHAR));
        System.out.println("false: "  + ip5.check("X", DataType.VARCHAR));

        IndependentPredicate ip6 = new IndependentPredicate(predicate6);
        ip6.print();
        System.out.println("false: "  + ip6.check("Y", DataType.VARCHAR));
        System.out.println("true: "  + ip6.check("X", DataType.VARCHAR));
    }
 */
