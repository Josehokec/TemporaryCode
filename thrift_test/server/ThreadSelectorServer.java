package thrift_test.server;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.layered.TFramedTransport;
import thrift_test.UserService;

public class ThreadSelectorServer {
    public static void main(String[] args) {
        try{
            TNonblockingServerSocket serverTransport = new TNonblockingServerSocket(9090);
            UserService.Processor processor = new UserService.Processor(new UserServiceImpl());

            TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
            //TCompactProtocol.Factory protocolFactory = new TCompactProtocol.Factory();
            TFramedTransport.Factory tTransport = new TFramedTransport.Factory();

            TThreadedSelectorServer.Args targs = new TThreadedSelectorServer.Args(serverTransport);

            // 绑定processor参数
            targs.processor(processor);
            targs.protocolFactory(protocolFactory);
            targs.transportFactory(tTransport);

            TServer server = new TThreadedSelectorServer(targs);
            System.out.println("Starting server...");
            server.serve();
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }
}
