// please run below command to generate java code
// thrift -gen java PushDownRPC.thrift

service PushDownRPC{
    binary initial(1:string tableName, 2:map<string, list<string>> ipMap)
}