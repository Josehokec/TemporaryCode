
// if one bit store a time unit, then occur out-of-memory exception
// thus, we set each bit store multiple time unit

service BitmapRPC{
    // load events based on independent predicates
    // return map<key: variable name, value: event number>
    map<string, i32> initial(1:string tableName, 2:map<string, list<string>> ipMap)

    // generate intervals that maybe contain matched results
    // return bitmap
    binary getReplayIntervals(1:string varName, 2:i64 window, 3:i32 headTailMarker)

    // without dependent predicates
    binary windowFilter(1:string variableName, 2:i64 window, 3:i32 headTailMarker, 4:binary intervalBitmap)

    binary getAllFilteredEvents(1:i64 window, 2:binary intervalBitmap)
}