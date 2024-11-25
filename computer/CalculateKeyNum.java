package computer;

import java.util.Arrays;

/**
 * we use this class to store statistical information
 */
public class CalculateKeyNum {

    public static int savedMaxCategoryNum = 10;

    public static double calculate(double[] pros, int sumCategoryNum, double averageEventNum){
        int caseNum = pros.length;
        double keySum = 0;
        double proSum = 0;
        for (double pro : pros) {
            proSum += pro;
            keySum += (1 - Math.pow(1 - pro, averageEventNum));
        }
        if(sumCategoryNum > caseNum){
            int remainingCategoryNum = sumCategoryNum - savedMaxCategoryNum;
            double avgRemainingPro = (1 - proSum) / remainingCategoryNum;
            keySum += remainingCategoryNum * (1 - Math.pow(1 - avgRemainingPro, averageEventNum));
        }
        return keySum;
    }

    public static int calCrimesDistrict(double estimateEventNum, double windowIdNum){
        double rowNum = 7701079;
        double averageEventNum = (estimateEventNum / windowIdNum);
        double[] counts = {
                518649, 492918, 451452, 446550, 438386,
                438380, 391229, 380828, 376911, 363931,
                //345263, 344821, 340036, 332292, 330714,
                //310445, 297793, 256938, 252175, 232518
        };

        if(counts.length != savedMaxCategoryNum){
            throw new RuntimeException("please adjust counts or savedMaxCategoryNum");
        }

        int districtCategoryNum = 24;
        double[] pros = new double[savedMaxCategoryNum];
        for(int i = 0; i < savedMaxCategoryNum; i++){
            pros[i] = counts[i] / rowNum;
        }

        //System.out.println("pros: " + Arrays.toString(pros));

        int keyNum = (int) (calculate(pros, districtCategoryNum, averageEventNum) * windowIdNum);
        System.out.println("keyNum: " + keyNum);
        return keyNum;
    }
    // estimated Event Num: 9328, windowIdNum: 2951.95

    public static int calCrimesBeat(double estimateEventNum, double windowIdNum){
        double rowNum = 7701079;
        double averageEventNum = (estimateEventNum / windowIdNum);
        double[] counts = {
                60235, 59706, 54577, 54321, 52788,
                52032, 51765, 51093, 48602, 48071,
                //47075, 46886, 46208, 44880, 44224,
                //44037, 42757, 42747, 42744, 42683
        };

        if(counts.length != savedMaxCategoryNum){
            throw new RuntimeException("please adjust counts or savedMaxCategoryNum");
        }

        int districtCategoryNum = 305;
        double[] pros = new double[savedMaxCategoryNum];
        for(int i = 0; i < savedMaxCategoryNum; i++){
            pros[i] = counts[i] / rowNum;
        }

        //System.out.println("pros: " + Arrays.toString(pros));

        int keyNum = (int) (calculate(pros, districtCategoryNum, averageEventNum) * windowIdNum);
        System.out.println("keyNum: " + keyNum);
        return keyNum;
    }

    public static int calSyntheticA3(double estimateEventNum, double windowIdNum){
        int a3CategoryNum = 50;
        double averageEventNum = (estimateEventNum / windowIdNum);
        double[] pros = {
                0.04147232049521569, 0.03475327101981663, 0.03133952545807808, 0.029122794002234793, 0.02751192538452083,
                0.026262119140468082, 0.0252498228708568, 0.024404526699457658, 0.02368244275240949, 0.023054639522173015,
                //0.022501071586134486, 0.022007317968829137, 0.021562682155293084, 0.021159026718422527, 0.02079002755798054,
                //0.02045067940180597, 0.020136957854505137, 0.019845582344029354, 0.019573846031506408, 0.019319491313988208
        };
        if(pros.length != savedMaxCategoryNum){
            throw new RuntimeException("please adjust counts or savedMaxCategoryNum");
        }

        int keyNum = (int) (calculate(pros, a3CategoryNum, averageEventNum) * windowIdNum);
        System.out.println("keyNum: " + keyNum);
        return keyNum;
    }

    public static int calSyntheticA4(double estimateEventNum, double windowIdNum){
        int a4CategoryNum = 50;
        double averageEventNum = (estimateEventNum / windowIdNum);
        double[] pros = {
                0.12449182594511901, 0.07663370800972048, 0.05769736219564887, 0.04717358074503648, 0.040351699790456864,
                0.035516892565955645, 0.031883956567188634, 0.029038745195863483, 0.026740595850874915, 0.024839384882996102,
                //0.023236245550072972, 0.02186321193097394, 0.020671904513499487, 0.01962687750151312, 0.018701522130829994,
                //0.01787544445921693, 0.017132730834116205, 0.016460767595659453, 0.015849417886951836, 0.015290434964812409
        };
        if(pros.length != savedMaxCategoryNum){
            throw new RuntimeException("please adjust counts or savedMaxCategoryNum");
        }
        int keyNum = (int) (calculate(pros, a4CategoryNum, averageEventNum) * windowIdNum);
        System.out.println("keyNum: " + keyNum);
        return keyNum;
    }

    public static int calDebug(double estimateEventNum, double windowIdNum){
        int a4CategoryNum = 20;
        double averageEventNum = (estimateEventNum / windowIdNum);
        double[] pros = {
                0.1827868981474484, 0.11251853424343868, 0.08471497455328313, 0.06926328241578408, 0.059246958454334066,
                0.052148183823965646, 0.046814073754156194, 0.04263655159806187, 0.03926226106082424, 0.03647078095436587,
                //0.034116948775116056, 0.03210097259047952, 0.03035181849199117, 0.028817442684173133, 0.027458776469687567,
                //0.026245876152123072, 0.02515537628425339, 0.024168756680161704, 0.02327113375520538, 0.02245039911114686
        };
        if(pros.length != savedMaxCategoryNum){
            throw new RuntimeException("please adjust counts or savedMaxCategoryNum");
        }
        int keyNum = (int) (calculate(pros, a4CategoryNum, averageEventNum) * windowIdNum);
        System.out.println("keyNum: " + keyNum);
        return keyNum;
    }

    public static void main(String[] args) {
        calCrimesDistrict(9328, 2951.95);
        calCrimesBeat(9328, 2951.95);
        calSyntheticA4(9328, 100);

    }
}
