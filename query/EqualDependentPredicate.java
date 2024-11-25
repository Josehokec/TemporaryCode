package query;

import store.DataType;

/**
 * this class support equal predicate like: v1.beat = v2.beat, v1.beat = v2.beat + 1
 * format:
 * leftVariableName.attributeName leftOperator leftConstValue = rightVariableName.attributeName rightOperator rightConstValue
 * Notice we require DataType(attributeName) = DataType(leftConstValue) = DataType(rightConstValue)
 */
public class EqualDependentPredicate extends DependentPredicate{
    public String leftVariableName;
    public ArithmeticOperator leftOperator;
    public String leftConstValue;
    public String rightVariableName;
    public ArithmeticOperator rightOperator;
    public String rightConstValue;
    public String attributeName;

    public EqualDependentPredicate(String singlePredicate){
        // case 1: v1.beat = v2.beat
        // case 2: v1.beat = v2.beat + 1
        // case 3: v1.beat + 1 = v2.beat + 1
        String[] twoParts = singlePredicate.split("=");

        String leftPart = twoParts[0].trim();
        int leftDotPos = -1;
        for(int i = 0; i < leftPart.length(); ++i){
            char ch = leftPart.charAt(i);
            if(ch == '.'){
                leftDotPos = i;
                leftVariableName = leftPart.substring(0,i);
            }
            if(leftDotPos != -1){
                switch (ch){
                    case '*':
                        leftOperator = ArithmeticOperator.MUL;
                        break;
                    case '/':
                        leftOperator = ArithmeticOperator.DIV;
                        break;
                    case '+':
                        leftOperator = ArithmeticOperator.ADD;
                        break;
                    case '-':
                        leftOperator = ArithmeticOperator.SUB;
                        break;
                }
                if(leftOperator != null){
                    attributeName = leftPart.substring(leftDotPos + 1, i).trim();
                    leftConstValue =leftPart.substring(i + 1).trim();
                    break;
                }
            }
        }
        if(leftOperator == null){
            attributeName = leftPart.substring(leftDotPos + 1);
        }


        String rightPart = twoParts[1].trim();
        int rightDotPos = -1;
        for(int i = 0; i < rightPart.length(); ++i){
            char ch = rightPart.charAt(i);
            if(ch == '.'){
                rightDotPos = i;
                rightVariableName = rightPart.substring(0,i);
            }
            if(rightDotPos != -1){
                switch (ch){
                    case '*':
                        rightOperator = ArithmeticOperator.MUL;
                        break;
                    case '/':
                        rightOperator = ArithmeticOperator.DIV;
                        break;
                    case '+':
                        rightOperator = ArithmeticOperator.ADD;
                        break;
                    case '-':
                        rightOperator = ArithmeticOperator.SUB;
                        break;
                }
                if(rightOperator != null){
                    String curAttributeName = rightPart.substring(leftDotPos + 1, i).trim();
                    if(!attributeName.equalsIgnoreCase(curAttributeName)){
                        throw new RuntimeException("two attribute names not equal, left is:" + attributeName + " right is: " + curAttributeName);
                    }
                    rightConstValue = rightPart.substring(i + 1).trim();
                    break;
                }
            }
        }
        if(rightOperator == null){
            String curAttributeName = rightPart.substring(rightDotPos + 1);
            if(!attributeName.equalsIgnoreCase(curAttributeName)){
                throw new RuntimeException("two attribute names not equal, " +
                        "left attribute name is: " + attributeName + " right attribute name is: " + curAttributeName);
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
        // before calling this function, you should ensure expression is correct
        boolean isAlign = varName1.equalsIgnoreCase(leftVariableName) && varName2.equalsIgnoreCase(rightVariableName);
        boolean isReverseAlign = varName2.equalsIgnoreCase(leftVariableName) && varName1.equalsIgnoreCase(rightVariableName);
        if(isAlign | isReverseAlign){
            if(isAlign){
                switch (dataType) {
                    case VARCHAR:
                        // if data type is string, we directly compare
                        // because we cannot support "XXX" + "YYY" this expression
                        String str1 = (String) o1;
                        String str2 = (String) o2;
                        return str1.equals(str2);
                    case INT:
                        return (int) getLeftValue(o1, dataType) == (int) getRightValue(o2, dataType);
                    case LONG:
                        return (long) getLeftValue(o1, dataType) == (long) getRightValue(o2, dataType);
                    case FLOAT:
                        return (float) getLeftValue(o1, dataType) == (float) getRightValue(o2, dataType);
                    default:
                        // DOUBLE:
                        return (double) getLeftValue(o1, dataType) == (double) getRightValue(o2, dataType);
                }
            }else{
                switch (dataType) {
                    case VARCHAR:
                        // if data type is string, we directly compare
                        // because we cannot support "XXX" + "YYY" this expression
                        String str1 = (String) o1;
                        String str2 = (String) o2;
                        return str1.equals(str2);
                    case INT:
                        // (int) getLeftValue(o1, dataType) == (int) getRightValue(o2, dataType);
                        return (int) getLeftValue(o2, dataType) == (int) getRightValue(o1, dataType);
                    case LONG:
                        return (long) getLeftValue(o2, dataType) == (int) getRightValue(o1, dataType);
                    case FLOAT:
                        return (float) getLeftValue(o2, dataType) == (int) getRightValue(o1, dataType);
                    default:
                        // DOUBLE
                        return (double) getLeftValue(o2, dataType) == (int) getRightValue(o1, dataType);
                }
            }
        }else{
            throw new RuntimeException("Cannot match this predicate.");
        }
    }

    @Override
    public boolean isEqualDependentPredicate() {
        return true;
    }

    @Override
    public boolean alignedCheck(Object o1, Object o2, DataType dataType) {
        switch (dataType) {
            case VARCHAR:
                // if data type is string, we directly compare
                // because we cannot support "XXX" + "YYY" this expression
                String str1 = (String) o1;
                String str2 = (String) o2;
                return str1.equals(str2);
            case INT:
                return (int) getLeftValue(o1, dataType) == (int) getRightValue(o2, dataType);
            case LONG:
                return (long) getLeftValue(o1, dataType) == (long) getRightValue(o2, dataType);
            case FLOAT:
                return (float) getLeftValue(o1, dataType) == (float) getRightValue(o2, dataType);
            default:
                // DOUBLE:
                return (double) getLeftValue(o1, dataType) == (double) getRightValue(o2, dataType);
        }
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

    /**
     * after obtaining the return result, we can use variableName.getClass().cast(obj)
     * @param value     -   number value
     * @param dataType  -   data type
     * @return          -   data value
     */
    public final Object getLeftValue(Object value, DataType dataType){
        // Object should be: STRING, INT, LONG, FLOAT, DOUBLE
        switch (dataType) {
            // we cannot support string +-x/
            case VARCHAR:
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
        // Object should be: STRING, INT, LONG, FLOAT, DOUBLE
        switch (dataType) {
            case VARCHAR:
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
        System.out.print(" = " + rightVariableName + "." + attributeName);
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

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append(leftVariableName).append(".").append(attributeName);
        if(leftOperator != null){
            switch (leftOperator){
                case ADD:
                    sb.append(" + ").append(leftConstValue);
                    break;
                case SUB:
                    sb.append(" - ").append(leftConstValue);
                    break;
                case MUL:
                    sb.append(" * ").append(leftConstValue);
                    break;
                default:
                    // DIV
                    sb.append(" / ").append(leftConstValue);
            }
        }
        sb.append(" = ").append(rightVariableName).append(".").append(attributeName);
        if(rightOperator != null){
            switch (rightOperator){
                case ADD:
                    sb.append(" + ").append(rightConstValue);
                    break;
                case SUB:
                    sb.append(" - ").append(rightConstValue);
                    break;
                case MUL:
                    sb.append(" * ").append(rightConstValue);
                    break;
                default:
                    // DIV
                    sb.append(" / ").append(rightConstValue);
            }
        }
        return sb.toString();
    }

    public ArithmeticOperator getLeftOperator() {
        return leftOperator;
    }

    public String getLeftConstValue() {
        return leftConstValue;
    }

    public ArithmeticOperator getRightOperator() {
        return rightOperator;
    }

    public String getRightConstValue() {
        return rightConstValue;
    }


}

/*
public static void main(String[] args){
        // case 1: v1.beat = v2.beat
        // case 2: v1.beat = v2.beat + 1
        // case 3: v1.beat + 1 = v2.beat + 1
        String predicate1 = "v1.beat = v2.beat";
        EqualDependentPredicate edp1 = new EqualDependentPredicate(predicate1);
        edp1.print();
        System.out.println("true: " + edp1.check("v1", 10, "v2", 10, DataType.INT));
        System.out.println("true: " + edp1.check("v1", 0, "v2", 0, DataType.INT));
        System.out.println("false: " + edp1.check("v1", 6, "v2", 3, DataType.INT));

        String predicate2 = "v1.beat = v2.beat + 1";
        EqualDependentPredicate edp2 = new EqualDependentPredicate(predicate2);
        edp2.print();
        System.out.println("true: " + edp2.check("v1", 6, "v2", 5, DataType.INT));
        System.out.println("true: " + edp2.check("v2", 6, "v1", 7, DataType.INT));

        String predicate3 = "v1.beat + 1 = v2.beat * 5";
        EqualDependentPredicate edp3 = new EqualDependentPredicate(predicate3);
        edp3.print();
        System.out.println("true: " + edp3.check("v1", 9, "v2", 2, DataType.INT));
        System.out.println("true: " + edp3.check("v2", 2, "v1", 9, DataType.INT));
        System.out.println("true: " + edp3.check("v1", -1, "v2", 0, DataType.INT));

        String predicate4 = "v1.str = v2.str";
        EqualDependentPredicate edp4 = new EqualDependentPredicate(predicate4);
        edp4.print();
        System.out.println("true: " + edp3.check("v1", "XXX", "v2", "XXX", DataType.VARCHAR));
        System.out.println("false: " + edp3.check("v1", "XXX", "v2", "YYY", DataType.VARCHAR));

        String predicate5 = "v1.a1 = v2.a2";
        EqualDependentPredicate edp5 = new EqualDependentPredicate(predicate5);
    }
 */
