package engine;

import event.DataType;
import event.Event;
import query.DependentPredicate;
import query.SelectionStrategy;

import java.util.*;

/**
 * expect start state, each state has a partial match buffer
 * partial match buffer in final state also called full match buffer
 */

public class State {
    private final String stateName;                 // state name (variable name)
    private final int stateId;                      // stateId can unique represent a state
    private final List<TransitionCondition> transitionConditions;   // edges / transactions
    private PartialMatchBuffer partialMatchBuffer;              // partial match buffer
    private boolean isFinal;                        // final state
    private boolean isStart;                        // start state

    public State(String stateName, int stateId){
        this.stateName = stateName;
        this.stateId = stateId;
        transitionConditions = new ArrayList<>();
        partialMatchBuffer = null;
        isFinal = false;
        isStart = false;
    }

    public boolean getIsFinal(){
        return isFinal;
    }

    public void setFinal() { isFinal = true; }

    public void setStart(){
        isStart = true;
    }

    public String getStateName(){
        return stateName;
    }

    public List<TransitionCondition> getTransactions() { return transitionConditions; }

    public void bindTransaction(TransitionCondition transitionCondition){
        transitionConditions.add(transitionCondition);
    }

    public void bindBuffer(PartialMatchBuffer buffer){
        this.partialMatchBuffer = buffer;
    }

    /**
     *
     * @param eventBuffer - nfa's event buffer
     * @param eventPointer - normally, we send event, here we use eventPointer to find the event
     * @param window - query window
     * @param strategy - selection strategy (currently, we only support SKIP_TILL_NEXT_MATCH and SKIP_TILL_ANY_MATCH)
     * @return - next state
     */
    public Set<State> transition(EventBuffer eventBuffer, int eventPointer, long window, SelectionStrategy strategy){
        Set<State> nextStates = new HashSet<>();

        Event event = eventBuffer.getEvent(eventPointer);
        long timestamp = event.getTimestamp();

        // for each transition (a state can transition multiple state)
        for(TransitionCondition transition : transitionConditions){
            // first check independent constraints
            if(transition.checkIndependentPredicate(event)){
                State nextState = transition.getNextState();
                boolean hasTransition = false;
                // here we need to judge whether current state whether is start state
                // if yes, then we directly add this event to next state's match buffer
                // otherwise, we need to check dependent predicate
                if(isStart){
                    // if this state is start state, then we generate a partial match
                    long startTime = timestamp;
                    long endTime = startTime;
                    List<Integer> eventPointers = new ArrayList<>(8);
                    eventPointers.add(eventPointer);
                    // generate a partial match
                    PartialMatch match = new PartialMatch(startTime, endTime, eventPointers);
                    // add the partial match to match buffer
                    PartialMatchBuffer nextMatchBuffer =  nextState.getPartialMatchBuffer();
                    if(nextMatchBuffer == null){
                        // create a buffer and bind to a state
                        List<String> stateNames = new ArrayList<>();
                        stateNames.add(nextState.stateName);
                        nextMatchBuffer = new PartialMatchBuffer(stateNames);
                        nextState.bindBuffer(nextMatchBuffer);
                    }
                    nextMatchBuffer.addPartialMatch(match);
                    hasTransition = true;
                }
                else{
                    List<PartialMatch> curPartialMatches = partialMatchBuffer.getPartialMatchList();

                    Iterator<PartialMatch> it = curPartialMatches.iterator();
                    // loop
                    while(it.hasNext()){
                        PartialMatch curMatch = it.next();
                        long matchStartTime = curMatch.getStartTime();
                        boolean timeout = timestamp - matchStartTime > window;
                        // firstly, we check time window whether timeout
                        // secondly, we check dependent predicate
                        if(timeout){
                            it.remove();
                        }
                        else{
                            boolean satisfyAllDP = true;
                            for(DependentPredicate dp : transition.getDependentPredicateList()){
                                String columnName = dp.getAttributeName();
                                DataType dataType = event.getDataType(columnName);

                                String nextVariableName = nextState.getStateName();
                                Object nextObject = event.getAttributeValue(columnName);
                                // break pointer
                                String leftVariableName = dp.getLeftVariableName();
                                String rightVariableName = dp.getRightVariableName();
                                if(rightVariableName.equals(nextVariableName)){
                                    // previous bug code: rightVariableName.equals(nextVariableName) && leftVariableName.equals(stateName)
                                    // e.g., v1.beat <= v3.beat
                                    int pos = partialMatchBuffer.findStateNamePosition(leftVariableName);
                                    Event curEvent = eventBuffer.getEvent(curMatch.getPointer(pos));
                                    satisfyAllDP = dp.alignedCheck(curEvent.getAttributeValue(columnName), nextObject, dataType);
                                }else if(leftVariableName.equals(nextVariableName)){
                                    // rightVariableName.equals(stateName) && leftVariableName.equals(nextVariableName)
                                    // e.g., v3.beat >= v1.beat
                                    int pos = partialMatchBuffer.findStateNamePosition(rightVariableName);
                                    Event curEvent = eventBuffer.getEvent(curMatch.getPointer(pos));
                                    satisfyAllDP = dp.alignedCheck(nextObject, curEvent.getAttributeValue(columnName), dataType);
                                }else{
                                    throw new RuntimeException("cannot match dependent predicate");
                                }

                                if(!satisfyAllDP) {
                                    break;
                                }
                            }

                            if(satisfyAllDP){
                                // selection strategy
                                if(strategy == SelectionStrategy.SKIP_TILL_NEXT_MATCH){
                                    // once can match then this partial match cannot match others events
                                    it.remove();
                                }else if(strategy == SelectionStrategy.STRICT_CONTIGUOUS){
                                    // to support STRICT_CONTIGUOUS strategy, we need add event sequence attribute
                                    System.out.println("We do not support this match strategy");
                                }
                                // create a match and add it to next buffer
                                List<Integer> newEventPointers = new ArrayList<>();
                                newEventPointers.addAll(curMatch.getEventPointers());
                                newEventPointers.add(eventPointer);
                                PartialMatch match = new PartialMatch(curMatch.getStartTime(), timestamp, newEventPointers);

                                PartialMatchBuffer nextBuffer = nextState.getPartialMatchBuffer();

                                if(nextBuffer == null){
                                    // create a buffer and bind to a state
                                    List<String> stateNames = new ArrayList<>(partialMatchBuffer.getStateNames());
                                    stateNames.add(nextState.getStateName());
                                    nextBuffer = new PartialMatchBuffer(stateNames);
                                    nextState.bindBuffer(nextBuffer);
                                }
                                nextBuffer.addPartialMatch(match);
                                hasTransition = true;
                            }
                        }
                    }
                }

                if(hasTransition){
                    nextStates.add(nextState);
                }
            }
        }
        return nextStates;
    }

    public PartialMatchBuffer getPartialMatchBuffer(){
        return partialMatchBuffer;
    }

    public String toString(){
        return " | stateId: " + stateId +
                " | stateName: " + stateName +
                " | isStart: " + isStart +
                " | isFinal: " + isFinal +
                " | transactionNum: " + transitionConditions.size() + " |";
    }

    public void recursiveDisplayState(){
        System.out.println("current state information: ");
        System.out.println(this);
        if(!isFinal){
            for(TransitionCondition t : transitionConditions){
                t.print();
                State nextState = t.getNextState();
                nextState.recursiveDisplayState();
            }
        }
    }
}
