package thrift_test.client;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import thrift_test.User;
import thrift_test.UserService;

public class TThreadSelectorClient {

    public static void main(String[] args) {
        for (int i = 0; i < 5; i++) {
            new Thread(()->{
                handle();
            }).start();
        }

        try {
            Thread.sleep(2000); // 休眠2秒
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("------");
        try{
            TTransport transport = new TFramedTransport(new TSocket("localhost", 9090));
            //TProtocol protocol = new TCompactProtocol(transport);
            TProtocol protocol = new TBinaryProtocol(transport);
            UserService.Client client = new UserService.Client(protocol);
            transport.open();
            boolean flag = client.isExist(String.valueOf(1));
            System.out.println("flag: " + flag);
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void handle() {
        TTransport transport = null;
        try{
            transport = new TFramedTransport(new TSocket("localhost", 9090));
            TProtocol protocol = new TBinaryProtocol(transport);
            //TProtocol protocol = new TCompactProtocol(transport);
            UserService.Client client = new UserService.Client(protocol);
            transport.open();
            User res = client.getByOd(1);
            //System.out.println(res);
            boolean flag = client.isExist(String.valueOf(1));
            System.out.println("flag: " + flag);
        }catch (Exception e) {
            System.out.println(e.getMessage());
        }finally {
            transport.close();
        }
    }
}
