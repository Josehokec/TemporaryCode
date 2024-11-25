package query;

import store.DataType;

/**
 * regex format of equal predicate:
 * varName1.attrName [[+-x/] C1]? = varName1.attrName [[+-x/] C2]?
 */
public abstract class DependentPredicate {
    public abstract String getLeftVariableName();
    public abstract String getAttributeName();
    public abstract String getRightVariableName();
    // because we want to implement a generic check function, so we use Object as argument
    public abstract boolean check(String varName1, Object o1, String varName2, Object o2, DataType dataType);
    public abstract boolean isEqualDependentPredicate();
    /**
     * to achieve a high performance check, we provide alignedCheck
     * call this method must ensure variable name has aligned
     * @param o1 - the value of left variable name bind attribute
     * @param o2 - the value of right variable name bind attribute
     * @param dataType  - data type
     * @return  - whether satisfy the dependent predicate
     */
    public abstract boolean alignedCheck(Object o1, Object o2, DataType dataType);

    public abstract Object getOneSideValue(String variableName, Object value, DataType dataType);

    public abstract void print();
}
