/*
designed by Josehokec
version: 1.0
date: 2024-11-12
*/

// please run below command to generate java code
// thrift -gen java FilterRPC.thrift


// this variable only store 16 bits
struct MinMaxPair{
    1: required i16 minValue;
    2: required i16 maxValue
}

struct {
    1: required string varName;
    2: required string colName;
    3: required double scale;
    4: map<i32, MinMaxPair> windowMinMaxValues
}

service FilterRPC{

    // load events based on independent predicates
    // input: table name, variables and its independent predicates
    // return: variable name and its number of events
    map<string, i32> initial(1:string tableName, 2:map<string, list<string>> ipStrMap)


    // generate replay interval set that maybe contain matched results
    // input: variable name, query window, the head or tail marker for this variable
    // return: replay intervals
    binary getReplayIntervals(1:string varName, 2:i64 window, 3:i32 headTailMarker)


    // get bloom filter for join operation, here we need to send shrink filter to filter
    // input: variable name, query window, dependent predicates map, expected number of keys for each bloom filter, shrink filter buffer
    // e.g., varName = t2, and dependent predicates are t1.a = t2.a and t1.b = t2.b
    // then dpStrMap: <t1, [t1.a = t2.a, t1.b = t2.b]>
    map<string, binary> getBF4EQJoin(1:string varName, 2:i64 window, 3:map<string, list<string>> dpStrMap, 4:map<string, i32> keyNumMap, 5:binary sfBuffer)


    // get min/max pair for non equal join operation
    map<string, MinMaxPair> getHashTable4NEQJoin(1:string varName, 2:i64 window, 3:list<string> neqDPStr, 4:binary sfBuffer)


    // if current variable and previous variables without dependent predicates, then we call this function
    // input: variable name, query window, dependent predicates map, the head or tail marker for this variable, shrink filter buffer
    // return: update shrink filter
    binary windowFilter(1:string varName, 2:i64 window, 3:i32 headTailMarker, 4:binary sfBuffer)


    // if current variable and previous variables have equal dependent predicates, then we call this function
    // input: variable name, query window, the head or tail marker for this variable, sequence map, dependent predicates map, bool filters map
    // return: update shrink filter
    binary eqJoinFilter(1:string varName, 2:i64 window, 3:i32 headTailMarker, 4:map<string, bool> previousOrNext, 5:map<string, list<string>> dpStrMap, 6:map<string, binary> bfBufferMap)


    // if current variable and previous variables have non equal dependent predicates, then we call this function
    // input: variable name, query window, dependent predicates map, the head or tail marker for this variable, shrink filter buffer
    // return: update shrink filter
    binary neqJoinFilter(1:string varName, 2:i64 window, 3:i32 headTailMarker, 4:map<string, bool> previousOrNext, 5:map<string, list<string>> dpStrMap, 6:map<string, MinMaxPair> valueRange)


    // pull filtered events
    // input: query window, shrink filter buffer
    // return: events
    binary getAllFilteredEvents(1:i64 window, 2:binary sfBuffer)
}

// please note that FilterUltra separates <fingerprint, interval_marker> and hit_markers
service FilterUltraRPC{
    // load events based on independent predicates
    // input: table name, variables and its independent predicates
    // return: variable name and its number of events
    map<string, i32> initial(1:string tableName, 2:map<string, list<string>> ipStrMap)


    // generate replay interval set that maybe contain matched results
    // input: variable name, query window, the head or tail marker for this variable
    // return: replay intervals
    binary getReplayIntervals(1:string varName, 2:i64 window, 3:i32 headTailMarker)

    // get bloom filter for join operation, here we need to send shrink filter to filter
    // input: variable name, query window, dependent predicates map, expected number of keys for each bloom filter, shrink filter buffer or updated markers
    // e.g., varName = t2, and dependent predicates are t1.a = t2.a and t1.b = t2.b
    // then dpStrMap: <t1, [t1.a = t2.a, t1.b = t2.b]>
    // if current variable is the second variable to access, then buff is shrink filter
    // otherwise, buffer is updated markers
    // return: bloom filters
    map<string, binary> getBF4EQJoin(1:string varName, 2:i64 window, 3:map<string, list<string>> dpStrMap, 4:map<string, i32> keyNumMap, 5:binary buff)

    // get min/max pair for non equal join operation
    // the reason why we send sfBuffer because current variable maybe second variable
    // that means only varName is the second variable, sfBuffer will not null
    binary getHashTable4NEQJoin(1:string varName, 2:i64 window, 3:list<string> neqDPStr, 4:binary buff)

    // if current variable and previous variables without dependent predicates, then we call this function
    // input: variable name, query window, dependent predicates map, the head or tail marker for this variable, shrink filter buffer or updated markers
    // if current variable is the second variable to access, then buff is shrink filter
    // otherwise, buffer is updated markers
    // return: update shrink filter
    binary windowFilter(1:string varName, 2:i64 window, 3:i32 headTailMarker, 4:binary buff)

    // if current variable and previous variables have equal dependent predicates, then we call this function
    // input: variable name, query window, the head or tail marker for this variable, sequence map, dependent predicates map, bool filters map
    // return: update shrink filter
    binary eqJoinFilter(1:string varName, 2:i64 window, 3:i32 headTailMarker, 4:map<string, bool> previousOrNext, 5:map<string, list<string>> dpStrMap, 6:map<string, binary> bfBufferMap)

    // if current variable and previous variables have non equal dependent predicates, then we call this function
    // input: variable name, query window, dependent predicates map, the head or tail marker for this variable, shrink filter buffer
    // return: update shrink filter
    binary neqJoinFilter(1:string varName, 2:i64 window, 3:i32 headTailMarker, 4:map<string, bool> previousOrNext, 5:map<string, list<string>> dpStrMap, 6:map<string, MinMaxPair> valueRange)

    // send filtered events
    binary getAllFilteredEvents(1:i64 window, 2:binary updatedMarkers)
}
