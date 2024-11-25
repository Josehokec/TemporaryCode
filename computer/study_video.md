## Thrift支持的类型

### 1.基本类型

### 2.容器
|Type|Details|Java|
|-|-|-|
|binary|未编码的字节序列|ByteBuffer|
|list<T>|数组|List|
|set<T>|集合|Set|
|map<K,V>|字典|Map|

### 3.类定义/结构体定义
```
struct <结构体名称>{
    <序号>:[字段性质] <字段类型> <字段名称> [=<默认值>][;|,]
}
```
- 支持嵌套（不能嵌套自己）
- 不能继承
- 每个字段名称需要正整数编号

**例子**
```
struct User{
    1: required string name, // 后缀可以是逗号，也可以是分号
    2: optional i32 age = 0;
    3: bool gender  //默认为optional
}
```

### 4.枚举、异常省略

### 5.Service/接口
```
service UserService{
    User getByOd(1:i32 id)
    bool isExist(1:string name)
}
```



## Thrift协议
传输协议分为文本和二进制协议，常见协议有下面四种
- TBinaryProtocol
- TCompactProtocol
- TJSONProtocol
- TSimpleJSONProtocol

### 传输层：
- TSocket
- TNonblockingTransport
- TFramedTransport





编写定义语言，然后生成类
`thrift -gen java user.thrift`

`thrift --out $(pwd) -gen java CERService.thrift`

如果不指定代码生成的目标目录，生成的类文件默认存放在gen-java目录下

只需要关注一下四个核心内部接口类：
- Iface：服务端通过实现XXXService.Iface接口，向客户端提供具体的同步业务逻辑
- AsynIface：服务端通过实现XXXService.Iface接口，向客户端提供具体的异步业务逻辑
- Client：客户端通过XXXService.Client的实例对象，以同步的方式访问服务端提供的服务方法
- AsynClient：客户端通过XXXService.Client的实例对象，以异步的方式访问服务端提供的服务方法

引入依赖
```
<dependency>
    <groupId>org.apache.thrift</groupId>
    <artifactId>libthrift</artifactId>
    <version>0.21.0</version>
</dependency>    
```

服务提供者实现XXXService.Iface接口，填写好接口函数

TServer子类：
- TSimpleServer:一般用于测试，一次只接受一个Socket连接
- TThreadPoolServer:可以处理多个客户端连接，不支持大量的连接（多线程非阻塞）
- TSaslNonblockingServer（单线程非阻塞）
- AbstractNonblockingServer 
   - 1.TNonblockingServer 单线程工作，采用NIO模式，传输通道使用TFramedTransport传输
     - THsHaServer 多线程非阻塞
   - 2.TThreadedSelectorServer 主从Reactor模型

