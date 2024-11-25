// Window-based push-pull communication based on interval set / array
// VLDB'2008_Plan-based complex event detection across distributed sources.
// DEBS'22_Predicate-Based Push-Pull Communication for Distributed CEP

// drawbacks: low parallelism && high query cost when events are unordered
service IntervalSetService{
    // load events based on independent predicates
    // return map<key: variable name, value: event number>
    map<string, i32> initial(1:string tableName, 2:map<string, list<string>> independentPredicateMap)

    // generate intervals that maybe contain matched results
    // return interval set (need to ensure sorted)
    binary generateIntervals(1:string variableName, 2:i64 window, 3:i32 headTailMarker)

    // return interval set (need to ensure sorted)
    binary filterBasedWindow(1:string variableName, 2:i64 window, 3:i32 headTailMarker, 4:binary intervalSet)

    // return filtered events
    binary getAllFilteredEvents(1:binary intervalSet)
}