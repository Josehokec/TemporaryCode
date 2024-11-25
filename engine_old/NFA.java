package engine;

import event.Event;
import query.*;


import java.util.*;

/**
 * Non-determine finite automata for complex event recognition
 */

public class NFA {
    private int stateNum;                           // number of states
    private HashMap<Integer, State> stateMap;       // all states
    private long window;                            // query window
    private Set<State> activeStates;                // active states
    private EventBuffer eventBuffer;                // event buffer
    private int consumeCount;                       // this variable is used for cleaning up active state

    public NFA(){
        stateNum = 0;
        consumeCount = 0;
        stateMap = new HashMap<>();
        activeStates = new HashSet<>();
        eventBuffer = new EventBuffer();
        window = Long.MAX_VALUE;
        State startState = createState("start", true, false);
        activeStates.add(startState);
    }

    /**
     * control this function to generate state
     * @param stateName - state name
     * @param isStart - mark start state
     * @param isFinal - mark final state
     */
    public State createState(String stateName, boolean isStart, boolean isFinal){
        // when create a new state, we need its state name and the length of a match
        State state = new State(stateName, stateNum);

        if(isStart){
            state.setStart();
        }
        if(isFinal){
            state.setFinal();
        }

        // store state, for start state its stateNum is 0
        stateMap.put(stateNum, state);
        stateNum++;
        return state;
    }

    /**
     * according to stateName find target state list
     * @param stateName - state name
     * @return - all states whose name is stateName
     */
    public List<State> getState(String stateName){
        // AND(Type1 a, Type2 b) -> a and b has two states
        List<State> states = new ArrayList<>();
        for (State state : stateMap.values()) {
            if(stateName.equals(state.getStateName())){
                states.add(state);
            }
        }
        return states;
    }

    public List<State> getFinalStates(){
        List<State> finalStates = new ArrayList<>();
        for(State state : stateMap.values()){
            if(state.getIsFinal()){
                finalStates.add(state);
            }
        }
        return finalStates;
    }

    public void constructNFA(PatternQuery pattern){
        this.window = pattern.getQueryWindow();
        if(pattern.onlyContainSEQ){
            // e.g., PATTERN SEQ(IBM a, Oracle b, IBM c, Oracle d)
            String[] seqStatement = pattern.getEventPattern().split("[()]");
            // seqEvent = "IBM a, Oracle b, IBM c, Oracle d"
            String[] seqEvent = seqStatement[1].split(",");
            String[] varNames = new String[seqEvent.length];
            String[] eventTypes = new String[seqEvent.length];

            for(int i = 0; i < seqEvent.length; ++i){
                String[] s = seqEvent[i].trim().split(" ");
                eventTypes[i] = s[0];
                varNames[i] = s[1].trim();
            }

            int varNum = varNames.length;

            // create all states
            for(int i = 0; i < varNum - 1; ++i){
                createState(varNames[i], false, false);
            }
            // final state
            createState(varNames[varNum - 1], false, true);

            Set<String> preVarName = new HashSet<>();
            // add all transaction
            for(int i = 0; i < varNum; ++i){
                String curVarName = varNames[i];
                List<IndependentPredicate> ipList = pattern.getIndependentPredicateList(curVarName);
                List<DependentPredicate> dpList = pattern.getRelatedDependentPredicate(preVarName, curVarName);
                addTransaction(stateMap.get(i), stateMap.get(i + 1), eventTypes[i], ipList, dpList);
                preVarName.add(curVarName);
            }
        }else{
            String patternStr = pattern.getEventPattern().substring(8);
            // here we decompose complex pattern to multiple sequential pattern
            List<String> seqQueries = DecomposeUtils.decomposingCEPOnlySEQ(patternStr);

            for(String query : seqQueries){
                // e.g., PATTERN SEQ(IBM a, Oracle b, IBM c, Oracle d)
                String[] seqStatement = query.split("[()]");
                // seqEvent = "IBM a, Oracle b, IBM c, Oracle d"
                String[] seqEvent = seqStatement[1].split(",");
                String[] varNames = new String[seqEvent.length];
                String[] eventTypes = new String[seqEvent.length];

                for(int i = 0; i < seqEvent.length; ++i){
                    String[] s = seqEvent[i].trim().split(" ");
                    eventTypes[i] = s[0];
                    varNames[i] = s[1].trim();
                }

                int varNum = varNames.length;

                List<State> createStates = new ArrayList<>(varNum);

                // create all states
                if(varNum == 1){
                    State curState = createState(varNames[0], false, true);
                    createStates.add(curState);
                }else{
                    for(int i = 0; i < varNum - 1; ++i){
                        State curState = createState(varNames[i], false, false);
                        createStates.add(curState);
                    }
                    // final state
                    State curState = createState(varNames[varNum - 1], false, true);
                    createStates.add(curState);
                }

                Set<String> preVarName = new HashSet<>();
                // add all transactions
                State preState = stateMap.get(0);
                for(int i = 0; i < varNum; ++i){
                    String curVarName = varNames[i];
                    List<IndependentPredicate> ipList = pattern.getIndependentPredicateList(curVarName);
                    List<DependentPredicate> dpList = pattern.getRelatedDependentPredicate(preVarName, curVarName);
                    State curState = createStates.get(i);
                    addTransaction(preState, curState, eventTypes[i], ipList, dpList);
                    preState = curState;
                    preVarName.add(curVarName);
                }
            }
        }
    }

    public void addTransaction(State curState, State nextState, String nextEventType,
                               List<IndependentPredicate> ipList, List<DependentPredicate> dpList){
        TransitionCondition transitionCondition = new TransitionCondition(nextEventType, ipList, dpList, nextState);
        curState.bindTransaction(transitionCondition);
    }

    public void consume(Event event, SelectionStrategy strategy){
        Set<State> allNextStates = new HashSet<>();
        // here we maintain an event buffer to buffer the event
        int eventPointer = eventBuffer.insertEvent(event);
        for(State state : activeStates){
            // final state cannot transact
            if(!state.getIsFinal()){
                // using match strategy
                Set<State> nextStates = state.transition(eventBuffer, eventPointer, window, strategy);
                allNextStates.addAll(nextStates);
            }
        }

        // add start state, maybe has performance bottle
        activeStates.addAll(allNextStates);

        consumeCount++;
        // Considering that when there are a large number of events,
        // most of the states of the state machine become active,
        // and some states do not have matching results,
        // we need to regularly clean up the active states
        // we clean up the active state per 100 events

        // below codes have serious bug
//        if(consumeCount == 100) {
//            Iterator<State> it = activeStates.iterator();
//            while(it.hasNext()) {
//                State state = it.next();
//                PartialMatchBuffer buffer = state.getPartialMatchBuffer();
//                if (buffer == null) {
//                    it.remove();
//                }else if(buffer.getPartialMatchSize() == 0){
//                    it.remove();
//                }
//            }
//            // we cannot use the following statement, because buffer may be null
//            // activeStates.removeIf(state -> state.getPartialMatchBuffer().getPartialMatchSize() == 0);
//            consumeCount = 0;
//        }
    }

    public void printFullMatch(){
        List<State> finalStateList = getFinalStates();
        int count = 0;
        System.out.println("Query result:");
        System.out.println("----------------");
        for(State s : finalStateList){
            PartialMatchBuffer buffer = s.getPartialMatchBuffer();
            if(buffer != null){
                List<PartialMatch> fullMatches = buffer.getPartialMatchList();
                if(fullMatches != null){
                    for(PartialMatch fullMatch : fullMatches){
                        count++;
                        fullMatch.print(eventBuffer);
                    }
                }
            }
        }
        System.out.println("result size: " + count);
        System.out.println("----------------");

    }

    public List<String> getAllMatchedResults(){
        List<String> allMatchedResults = new ArrayList<>(128);
        List<State> finalStateList = getFinalStates();
        for(State s : finalStateList){
            PartialMatchBuffer buffer = s.getPartialMatchBuffer();
            if(buffer != null){
                List<PartialMatch> fullMatches = buffer.getPartialMatchList();
                if(fullMatches != null){
                    for(PartialMatch fullMatch : fullMatches){
                        String result = fullMatch.getSingleMatchedResult(eventBuffer);
                        allMatchedResults.add(result);
                    }
                }
            }
        }
        return allMatchedResults;
    }

    public void printActiveStates(){
        for(State state : activeStates){
            System.out.println(state);
        }
    }

    public void getFullMatchEventsStatistic(){
        HashMap<String, Set<Integer>> infoMap = new HashMap<>();

        List<State> finalStateList = getFinalStates();
        for(State s : finalStateList){
            PartialMatchBuffer buffer = s.getPartialMatchBuffer();
            if(buffer != null){
                List<PartialMatch> fullMatches = buffer.getPartialMatchList();
                if(fullMatches != null){
                    List<String> stateNames = buffer.getStateNames();
                    // initialization
                    for(String stateName : stateNames){
                        if(!infoMap.containsKey(stateName)){
                            infoMap.put(stateName, new HashSet<>(1024));
                        }
                    }
                    // insert pointers
                    int size = stateNames.size();
                    for(PartialMatch fullMatch : fullMatches){
                        List<Integer> pointers = fullMatch.getEventPointers();
                        for(int i = 0; i < size; ++i){
                            int pointer = pointers.get(i);
                            infoMap.get(stateNames.get(i)).add(pointer);
                        }
                    }
                }
            }
        }

        for(Map.Entry<String,Set<Integer>> entry : infoMap.entrySet()){
            String varName = entry.getKey();
            int size = entry.getValue().size();
            System.out.println("varName: " + varName + " size: " + size);
        }
    }

    public void display(){
        State initialState = stateMap.get(0);
        System.out.println("--------All NFA paths-------");
        int cnt = 1;
        for(TransitionCondition t : initialState.getTransactions()){
            System.out.println("path number: " + cnt);
            System.out.println("start state: " + initialState);
            cnt++;
            t.print();
            State nextState = t.getNextState();
            nextState.recursiveDisplayState();
        }

    }
}


/*
// a simple example (without dependent predicate)
String queryStatement = "PATTERN SEQ(Type1 a, Type2 b, Type3 c)\nUSING SKIP_TILL_ANY_MATCH\nWHERE 35 <= a.price <= 40 AND 110 <= b.price <= 113 AND c.price >= 100 AND c.volume <= 100\nWITHIN 100 units";

SequentialPattern pattern = (SequentialPattern) QueryParse.parseQueryString(queryStatement);
this.window = pattern.getQueryWindow();
// create all state
createState("A", false, false);
createState("B", false, false);
createState("C", false, true);

// add all transaction

// start state -> state 'a'
List<State> stateA = getState("A");
State startState = stateMap.get(0);
for(State state : stateA){
    addTransaction(startState, state, "B", pattern.getIndependentPredicateList("A"), new ArrayList<>());
}

// state 'a' -> state 'b'
List<State> stateB = getState("B");
for(State state1 : stateA){
    for(State state2 : stateB){
        addTransaction(state1, state2, "S", pattern.getIndependentPredicateList("B"), new ArrayList<>());
    }
}

// state 'b' -> state 'c'
List<State> stateC = getState("C");
for(State state1 : stateB){
    for(State state2 : stateC){
        addTransaction(state1, state2, "B", pattern.getIndependentPredicateList("C"), new ArrayList<>());
    }
}
 */