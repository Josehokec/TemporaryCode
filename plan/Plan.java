package plan;

import java.util.ArrayList;
import java.util.List;

/**
 * plan format: [variableName1, ..., variableName_n]
 */
public class Plan {
    private List<String> steps;

    public Plan(int len){
        steps = new ArrayList<>(len);
    }

    public Plan(List<String> steps) {
        this.steps = steps;
    }

    public List<String> getSteps() {
        return steps;
    }

    public void add(String step){
        steps.add(step);
    }

//    public String getVariableName(int pos){
//        return plan.get(pos);
//    }
}
