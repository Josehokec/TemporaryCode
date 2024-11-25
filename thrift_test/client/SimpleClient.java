package thrift_test.client;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import thrift_test.User;
import thrift_test.UserService;

public class SimpleClient {
    public static void main(String[] args) {
        f1();
        System.out.println("---------");
        try{
            //使用阻塞式IO
            TTransport transport = new TSocket("localhost", 9090);
            // 指定二进制编码格式
            TProtocol protocol = new TBinaryProtocol(transport);
            UserService.Client client = new UserService.Client(protocol);
            // 建立连接
            transport.open();
            System.out.println("my test: " + client.isExist(String.valueOf(2)));
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

    public static void f1(){
        try{
            //使用阻塞式IO
            TTransport transport = new TSocket("localhost", 9090);
            // 指定二进制编码格式
            TProtocol protocol = new TBinaryProtocol(transport);
            UserService.Client client = new UserService.Client(protocol);
            // 建立连接
            transport.open();
            //发起rpc调用
            User res = client.getByOd(1);
            System.out.println(res);
            res = client.getByOd(2);
            System.out.println(res);
            res = client.getByOd(3);
            System.out.println(res);
            transport.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
