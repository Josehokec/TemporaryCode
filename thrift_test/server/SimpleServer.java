package thrift_test.server;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TServer;
import thrift_test.UserService;

public class SimpleServer {
    public static void main(String[] args) {
        try{
            TServerTransport serverTransport = new TServerSocket(9090);
            UserService.Processor processor = new UserService.Processor(new UserServiceImpl());
            TBinaryProtocol.Factory binaryProtocol = new TBinaryProtocol.Factory();

            TSimpleServer.Args targs = new TSimpleServer.Args(serverTransport);
            targs.processor(processor);
            targs.protocolFactory(binaryProtocol);
            TServer server = new TSimpleServer(targs);
            System.out.println("Starting server...");
            server.serve();
        }catch (
                Exception e
        ){
            e.printStackTrace();
        }
    }
}
