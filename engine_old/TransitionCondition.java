package engine;

import event.DataType;
import query.DependentPredicate;
import query.IndependentPredicate;
import event.Event;

import java.util.ArrayList;
import java.util.List;

public class TransitionCondition {
    private String nextEventType;                   // event type
    private List<IndependentPredicate> ipList;     //  independent constraint list
    private List<DependentPredicate> dpList;       // dependent constraint list
    private State nextState;                        // next state

    public TransitionCondition(String nextEventType){
        this.nextEventType = nextEventType;
        this.ipList = new ArrayList<>();
        this.dpList = new ArrayList<>();
    }

    public TransitionCondition(String nextEventType, List<IndependentPredicate> ipList, List<DependentPredicate> dpList, State nextState){
        this.nextEventType = nextEventType;
        this.ipList = ipList;
        this.dpList = dpList;
        this.nextState = nextState;
    }


    public boolean checkIndependentPredicate(Event event){
        // determine the stored column index for event type
        String curType = event.getEventType();
        // we first check event type
        if (curType.equals(nextEventType)) {
            for(IndependentPredicate ip : ipList){
                String columnName = ip.getAttributeName();
                DataType dataType = event.getDataType(columnName);
                // satisfy = ip.check(dataType);
                boolean satisfy = ip.check(event.getAttributeValue(columnName), dataType);
                if(!satisfy){
                    return false;
                }
            }
            return true;
        }else{
            return false;
        }
    }

    public List<DependentPredicate> getDependentPredicateList(){
        return dpList;
    }

    public State getNextState(){
        return nextState;
    }

    public void print(){
        System.out.println("transaction infomartion");
        System.out.println("event type: " + nextEventType);
        System.out.println("ic list: ");
        for(IndependentPredicate ip : ipList){
            ip.print();
        }
        System.out.println("dc list: ");
        for(DependentPredicate dp : dpList){
            dp.print();
        }
        System.out.println("next state information: ");
        System.out.println(nextState);
        System.out.println("\n");
    }
}

