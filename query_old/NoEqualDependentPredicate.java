package query;

import event.DataType;

/**
 * support format:
 * leftVariableName.attributeName leftOperator leftConstValue [><][=]?|[!=] rightVariableName.attributeName rightOperator rightConstValue
 */

public class NoEqualDependentPredicate extends DependentPredicate{
    public String leftVariableName;
    public ArithmeticOperator leftOperator;
    public String leftConstValue;
    public ComparedOperator cmp;           // '>' | '>=' | '<' | '<=' | '!='
    public String rightVariableName;
    public ArithmeticOperator rightOperator;
    public String rightConstValue;
    public String attributeName;

    NoEqualDependentPredicate(String singlePredicate){
        int leftDotPos = -1;
        int rightDotPos = -1;

        // we use it to mark whether we finish left part decoding
        int finishLeftPart = -1;
        int leftOperatorPos = -1;

        leftOperator = null;
        rightOperator = null;

        String str = singlePredicate.trim();
        int len = str.length();
        for(int i = 0; i < len; ++i){
            char ch = str.charAt(i);

            if(ch == '.'){
                // if '.' is the first dot
                if(leftDotPos == -1){
                    leftDotPos = i;
                    leftVariableName = str.substring(0, i);
                }else{
                    rightDotPos = i;
                    rightVariableName = str.substring(finishLeftPart, i).trim();
                }

            }
            // '+' | '-' | '*' | '/'
            if(ch == '+' || ch == '-' || ch == '*' || ch == '/'){
                ArithmeticOperator curArithmeticOperator;
                switch (ch){
                    case '+':
                        curArithmeticOperator = ArithmeticOperator.ADD;
                        break;
                    case '-':
                        curArithmeticOperator = ArithmeticOperator.SUB;
                        break;
                    case '*':
                        curArithmeticOperator = ArithmeticOperator.MUL;
                        break;
                    default:
                        // -> '/'
                        curArithmeticOperator = ArithmeticOperator.DIV;
                }
                if(finishLeftPart == -1){
                    leftOperatorPos = i;
                    attributeName = str.substring(leftDotPos + 1, i).trim();
                    leftOperator = curArithmeticOperator;
                }else{
                    String rightAttributeName = str.substring(rightDotPos + 1, i).trim();
                    if(!attributeName.equals(rightAttributeName)){
                        throw new RuntimeException("Two attribute name is different, left attribute name: " +
                                attributeName + ", right attribute name: " + rightAttributeName);
                    }
                    rightOperator = curArithmeticOperator;
                    rightConstValue = str.substring(i + 1).trim();
                    break;
                }
            }

            // '>' | '>=' | '<' | <=' | '!='
            if(ch == '>' || ch == '<' || ch == '!'){
                int x = 5;
                if(leftOperator == null){
                    leftConstValue = null;
                    String temp = str.substring(leftDotPos + 1, i);
                    attributeName = temp.trim();
                }else{
                    leftConstValue = str.substring(leftOperatorPos + 1, i).trim();
                }
                char nextChar = str.charAt(i + 1);
                if(nextChar == '='){
                    switch (ch){
                        case '>':
                            cmp = ComparedOperator.GE;
                            break;
                        case '<':
                            cmp = ComparedOperator.LE;
                            break;
                        default:
                            // '!'
                            cmp = ComparedOperator.NEQ;
                    }
                    finishLeftPart = i + 2;
                }else{
                    switch (ch){
                        case '>':
                            cmp = ComparedOperator.GT;
                            break;
                        case '<':
                            cmp = ComparedOperator.LT;
                    }
                    finishLeftPart = i + 1;
                }
            }
        }
        // solve case 4: v1.beat + 1 > v2.beat
        if(rightOperator == null){
            String rightAttributeName = str.substring(rightDotPos + 1);
            if(!attributeName.equals(rightAttributeName)){
                throw new RuntimeException("Two attribute name is different, left attribute name: " +
                        attributeName + ", right attribute name: " + rightAttributeName);
            }
        }

    }

    @Override
    public String getLeftVariableName() {
        return leftVariableName;
    }

    @Override
    public String getAttributeName() {
        return attributeName;
    }

    @Override
    public String getRightVariableName() {
        return rightVariableName;
    }

    @Override
    public boolean check(String varName1, Object o1, String varName2, Object o2, DataType dataType) {
        boolean isAlign = varName1.equalsIgnoreCase(leftVariableName) && varName2.equalsIgnoreCase(rightVariableName);
        boolean isReverseAlign = varName2.equalsIgnoreCase(leftVariableName) && varName1.equalsIgnoreCase(rightVariableName);
        int comparedResult;
        if(isAlign | isReverseAlign){
            // NEQ
            if(isAlign){
                switch (dataType) {
                    case STRING:
                        // if data type is string, we directly compare
                        // because we cannot support "XXX" + "YYY" this expression
                        String str1 = (String) o1;
                        String str2 = (String) o2;
                        comparedResult = str1.compareTo(str2);
                        break;
                    case INT:
                        Integer intValue =  (int) getLeftValue(o1, dataType);
                        comparedResult = intValue.compareTo((int) getRightValue(o2, dataType));
                        break;
                    case LONG:
                        Long longValue = (long) getLeftValue(o1, dataType);
                        comparedResult = longValue.compareTo((long) getRightValue(o2, dataType));
                        break;
                    case FLOAT:
                        Float floatValue = (float) getLeftValue(o1, dataType);
                        comparedResult = floatValue.compareTo((float) getRightValue(o2, dataType));
                        break;
                    default:
                        // DOUBLE:
                        Double doubleValue =  (double) getLeftValue(o1, dataType);
                        comparedResult = doubleValue.compareTo((double) getRightValue(o2, dataType));
                }
            }else{
                switch (dataType) {
                    case STRING:
                        // if data type is string, we directly compare
                        // because we cannot support "XXX" + "YYY" this expression
                        // reverse
                        String str1 = (String) o2;
                        String str2 = (String) o1;
                        comparedResult = str1.compareTo(str2);
                        break;
                    case INT:
                        // (int) getLeftValue(o1, dataType) == (int) getRightValue(o2, dataType);
                        Integer intValue = (int) getLeftValue(o2, dataType);
                        comparedResult = intValue.compareTo((int) getRightValue(o1, dataType));
                        break;
                    case LONG:
                        Long longValue = (long) getLeftValue(o2, dataType);
                        comparedResult = longValue.compareTo((long) getRightValue(o1, dataType));
                        break;
                    case FLOAT:
                        Float floatValue = (float) getLeftValue(o2, dataType);
                        comparedResult = floatValue.compareTo((float) getRightValue(o1, dataType));
                        break;
                    default:
                        // DOUBLE
                        Double doubleValue = (double) getLeftValue(o2, dataType);
                        comparedResult = doubleValue.compareTo((double) getRightValue(o1, dataType));
                }
            }
            switch (cmp){
                case GT:
                    return comparedResult > 0;
                case GE:
                    return comparedResult >= 0;
                case LT:
                    return comparedResult < 0;
                case LE:
                    return comparedResult <= 0;
                default:
                    // NEQ
                    return comparedResult != 0;
            }
        }else{
            throw new RuntimeException("Cannot match this predicate.");
        }
    }

    @Override
    public boolean isEqualDependentPredicate() {
        return false;
    }

    @Override
    public Object getOneSideValue(String variableName, Object value, DataType dataType) {
        if(variableName.equals(leftVariableName)){
            return getLeftValue(value, dataType);
        }else if(variableName.equals(rightVariableName)){
            return getRightValue(value, dataType);
        }else{
            throw new RuntimeException("This dependent predicate does not contain variable name '" + variableName + "'");
        }
    }

    @Override
    public boolean alignedCheck(Object o1, Object o2, DataType dataType) {
        int comparedResult;
        switch (dataType) {
            case STRING:
                // if data type is string, we directly compare
                // because we cannot support "XXX" + "YYY" this expression
                String str1 = (String) o1;
                String str2 = (String) o2;
                comparedResult = str1.compareTo(str2);
                break;
            case INT:
                Integer intValue =  (int) getLeftValue(o1, dataType);
                comparedResult = intValue.compareTo((int) getRightValue(o2, dataType));
                break;
            case LONG:
                Long longValue = (long) getLeftValue(o1, dataType);
                comparedResult = longValue.compareTo((long) getRightValue(o2, dataType));
                break;
            case FLOAT:
                Float floatValue = (float) getLeftValue(o1, dataType);
                comparedResult = floatValue.compareTo((float) getRightValue(o2, dataType));
                break;
            default:
                // DOUBLE:
                Double doubleValue =  (double) getLeftValue(o1, dataType);
                comparedResult = doubleValue.compareTo((double) getRightValue(o2, dataType));
        }
        switch (cmp){
            case GT:
                return comparedResult > 0;
            case GE:
                return comparedResult >= 0;
            case LT:
                return comparedResult < 0;
            case LE:
                return comparedResult <= 0;
            default:
                // NEQ
                return comparedResult != 0;
        }
    }

    public final Object getLeftValue(Object value, DataType dataType){
        switch (dataType) {
            case STRING:
                return value;
            case INT:
                if(leftOperator == null){
                    return value;
                }
                switch (leftOperator) {
                    case ADD:
                        return (int) ((int) value + Double.parseDouble(leftConstValue));
                    case SUB:
                        return (int) ((int) value - Double.parseDouble(leftConstValue));
                    case MUL:
                        return (int) ((int) value * Double.parseDouble(leftConstValue));
                    default:
                        // DIV
                        return (int) ((int) value / Double.parseDouble(leftConstValue));
                }
            case LONG:
                if(leftOperator == null){
                    return value;
                }
                switch (leftOperator) {
                    case ADD:
                        return (long) ((long) value + Double.parseDouble(leftConstValue));
                    case SUB:
                        return (long) ((long) value - Double.parseDouble(leftConstValue));
                    case MUL:
                        return (long) ((long) value * Double.parseDouble(leftConstValue));
                    default:
                        // DIV
                        return (long) ((long) value / Double.parseDouble(leftConstValue));
                }
            case FLOAT:
                if(leftOperator == null){
                    return value;
                }
                switch (leftOperator) {
                    case ADD:
                        return (float) ((float) value + Double.parseDouble(leftConstValue));
                    case SUB:
                        return (float) ((float) value - Double.parseDouble(leftConstValue));
                    case MUL:
                        return (float) ((float) value * Double.parseDouble(leftConstValue));
                    default:
                        // DIV
                        return (float) ((float) value / Double.parseDouble(leftConstValue));
                }
            default:
                // DOUBLE
                if(leftOperator == null){
                    return value;
                }
                switch (leftOperator) {
                    case ADD:
                        return (double) value + Double.parseDouble(leftConstValue);
                    case SUB:
                        return (double) value - Double.parseDouble(leftConstValue);
                    case MUL:
                        return (double) value * Double.parseDouble(leftConstValue);
                    default:
                        // DIV
                        return (double) value / Double.parseDouble(leftConstValue);
                }
        }
    }

    public final Object getRightValue(Object value, DataType dataType){
        switch (dataType) {
            case STRING:
                return value;
            case INT:
                if(rightOperator == null){
                    return value;
                }
                switch (rightOperator) {
                    case ADD:
                        return (int) ((int) value + Double.parseDouble(rightConstValue));
                    case SUB:
                        return (int) ((int) value - Double.parseDouble(rightConstValue));
                    case MUL:
                        return (int) ((int) value * Double.parseDouble(rightConstValue));
                    default:
                        // DIV
                        return (int) ((int) value / Double.parseDouble(rightConstValue));
                }
            case LONG:
                if(rightOperator == null){
                    return value;
                }
                switch (rightOperator) {
                    case ADD:
                        return (long) ((long) value + Double.parseDouble(rightConstValue));
                    case SUB:
                        return (long) ((long) value - Double.parseDouble(rightConstValue));
                    case MUL:
                        return (long) ((long) value * Double.parseDouble(rightConstValue));
                    default:
                        // DIV
                        return (long) ((long) value / Double.parseDouble(rightConstValue));
                }
            case FLOAT:
                if(rightOperator == null){
                    return value;
                }
                switch (rightOperator) {
                    case ADD:
                        return (float) ((float) value + Double.parseDouble(rightConstValue));
                    case SUB:
                        return (float) ((float) value - Double.parseDouble(rightConstValue));
                    case MUL:
                        return (float) ((float) value * Double.parseDouble(rightConstValue));
                    default:
                        // DIV
                        return (float) ((float) value / Double.parseDouble(rightConstValue));
                }
            default:
                // DOUBLE
                if(rightOperator == null){
                    return value;
                }
                switch (rightOperator) {
                    case ADD:
                        return (double) value + Double.parseDouble(rightConstValue);
                    case SUB:
                        return (double) value - Double.parseDouble(rightConstValue);
                    case MUL:
                        return (double) value * Double.parseDouble(rightConstValue);
                    default:
                        // DIV
                        return (double) value / Double.parseDouble(rightConstValue);
                }
        }
    }

    @Override
    public void print() {
        System.out.print(leftVariableName + "." + attributeName);
        if(leftOperator != null){
            switch (leftOperator){
                case ADD:
                    System.out.print(" + " + leftConstValue);
                    break;
                case SUB:
                    System.out.print(" - " + leftConstValue);
                    break;
                case MUL:
                    System.out.print(" * " + leftConstValue);
                    break;
                default:
                    // DIV
                    System.out.print(" / " + leftConstValue);
            }
        }
        // compared operator
        switch (cmp){
            case GT:
                System.out.print(" > ");
                break;
            case GE:
                System.out.print(" >= ");
                break;
            case LT:
                System.out.print(" < ");
                break;
            case LE:
                System.out.print(" <= ");
                break;
            case NEQ:
                System.out.print(" != ");
                break;
            default:
                throw new RuntimeException("this compared operator is illegal");
        }

        System.out.print(rightVariableName + "." + attributeName);
        if(rightOperator != null){
            switch (rightOperator){
                case ADD:
                    System.out.print(" + " + rightConstValue);
                    break;
                case SUB:
                    System.out.print(" - " + rightConstValue);
                    break;
                case MUL:
                    System.out.print(" * " + rightConstValue);
                    break;
                default:
                    // DIV
                    System.out.print(" / " + rightConstValue);
            }
        }
        System.out.println();
    }

    public static void main(String[] args){
        // case 1: v1.beat > v2.beat
        // case 2: v1.beat != v2.beat + 1
        // case 3: v1.beat + 1 <= v2.beat * 1
        // case 4: v1.beat + 1 > v2.beat

        String predicate1 = "v1.beat > v2.beat";
        NoEqualDependentPredicate nedp1 = new NoEqualDependentPredicate(predicate1);
        nedp1.print();
        System.out.println("true: " + nedp1.check("v1", 10, "v2", 9, DataType.INT));
        System.out.println("false: " + nedp1.check("v1", 10, "v2", 10, DataType.INT));
        System.out.println("true: " + nedp1.check("v2", 10, "v1", 11, DataType.INT));


        String predicate2 = "v1.beat != v2.beat + 1";
        NoEqualDependentPredicate nedp2 = new NoEqualDependentPredicate(predicate2);
        nedp2.print();
        System.out.println("false: " + nedp2.check("v1", 6f, "v2", 5f, DataType.FLOAT));
        System.out.println("false: " + nedp2.check("v2", 6f, "v1", 7f, DataType.FLOAT));
        System.out.println("true: " + nedp2.check("v1", 6f, "v2", 6f, DataType.FLOAT));


        String predicate3 = "v1.beat + 1 <= v2.beat * 2.5";
        NoEqualDependentPredicate nedp3 = new NoEqualDependentPredicate(predicate3);
        nedp3.print();
        System.out.println("true: " + nedp3.check("v1", 9.0, "v2", 4.0, DataType.DOUBLE));
        System.out.println("true: " + nedp3.check("v2", 2.0, "v1", 0.0, DataType.DOUBLE));
        System.out.println("true: " + nedp3.check("v1", -1.0, "v2", 1.0, DataType.DOUBLE));

        String predicate4 = "v1.beat + 1 > v2.beat";
        NoEqualDependentPredicate nedp4 = new NoEqualDependentPredicate(predicate4);
        nedp4.print();
        System.out.println("true: " + nedp3.check("v1", 9.0, "v2", 9.0, DataType.DOUBLE));
        System.out.println("true: " + nedp3.check("v2", 2.0, "v1", 0.0, DataType.DOUBLE));

        String predicate5 = "v1.str != v2.str";
        NoEqualDependentPredicate edp5 = new NoEqualDependentPredicate(predicate5);
        edp5.print();
        System.out.println("false: " + edp5.check("v1", "XXX", "v2", "XXX", DataType.STRING));
        System.out.println("true: " + edp5.check("v1", "XXX", "v2", "YYY", DataType.STRING));
    }

    public ArithmeticOperator getLeftOperator() {
        return leftOperator;
    }

    public String getLeftConstValue() {
        return leftConstValue;
    }

    public ComparedOperator getCmp() {
        return cmp;
    }

    public ArithmeticOperator getRightOperator() {
        return rightOperator;
    }

    public String getRightConstValue() {
        return rightConstValue;
    }
}
