package filter;


/**
 * 用于非等值谓词的三元组
 * 存储 [windowId,min,max]
 * 倒时候： 变量名-属性名-缩放因子
 */

public class Triple {
    long windowId;
    int minValue;
    int maxValue;

}
