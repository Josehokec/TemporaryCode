package engine;

import java.util.ArrayList;
import java.util.List;


/**
 * Notice we do not support
 */

public class PartialMatchBuffer {
    // private int length;
    private List<String> stateNames;
    private List<PartialMatch> partialMatchList;

    public PartialMatchBuffer(List<String> stateNames){
        // this.length = length;
        this.stateNames = stateNames;
        partialMatchList = new ArrayList<>(512);
    }

    public List<PartialMatch> getPartialMatchList(){
        return partialMatchList;
    }

    public int findStateNamePosition(String stateName){
        for(int i = 0; i < stateNames.size(); ++i){
            if(stateNames.get(i).equals(stateName)){
                return i;
            }
        }
        throw new RuntimeException("cannot find stateName: " + stateName);
    }

    public void addPartialMatch(PartialMatch match){
        partialMatchList.add(match);
    }

    public List<String> getStateNames(){
        return stateNames;
    }

    public int getPartialMatchSize(){
        return partialMatchList.size();
    }
}
