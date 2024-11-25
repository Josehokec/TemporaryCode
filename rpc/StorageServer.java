package rpc;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.layered.TFramedTransport;
import rpc.iface.BitmapRPC;
import rpc.iface.FilterRPC;
import rpc.iface.FilterUltraRPC;


public class StorageServer {
    public static void main(String[] args) {
        //StorageServer.startService("FILTER_BASED");
        StorageServer.startService("FILTER_ULTRA_BASED");
        //StorageServer.startService("INTERVAL_SET_BASED");
        //StorageServer.startService("BITMAP_BASED");
    }

    public static void startService(String serviceName){
        TNonblockingServerSocket serverTransport = null;
        try{
            int clientTimeOut = 180_000; // 3min
            serverTransport = new TNonblockingServerSocket(9090, clientTimeOut);
            TThreadedSelectorServer.Args targs = new TThreadedSelectorServer.Args(serverTransport);

            switch(serviceName){
                case "FILTER_BASED":
                    FilterRPC.Processor processor1 = new FilterRPC.Processor(new FilterRPCImpl());
                    targs.processor(processor1);
                    break;
                case "FILTER_ULTRA_BASED":
                    FilterUltraRPC.Processor processor2 = new FilterUltraRPC.Processor(new FilterUltraRPCImpl());
                    targs.processor(processor2);
                    break;
                case "BITMAP_BASED":
                    BitmapRPC.Processor processor3 = new BitmapRPC.Processor(new BitmapRPCImpl());
                    targs.processor(processor3);
                    break;
                case "INTERVAL_SET_BASED":
                    IntervalSetService.Processor processor4 = new IntervalSetService.Processor(new IntervalSetServiceImpl());
                    targs.processor(processor4);
                    break;
                default:
                    //...
            }

            TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
            targs.protocolFactory(protocolFactory);

            TFramedTransport.Factory tTransport = new TFramedTransport.Factory();
            targs.transportFactory(tTransport);

            TServer server = new TThreadedSelectorServer(targs);
            System.out.println("Starting filter service...");
            server.serve();
        }catch (Exception e){
           e.printStackTrace();
        }finally {
            serverTransport.close();
        }
    }
}
