package rpc;


import org.roaringbitmap.longlong.Roaring64Bitmap;
// import org.roaringbitmap.RoaringBitmap;
import rpc.iface.BitmapRPC;
import store.EventCache;
import store.FullScan;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class BitmapRPCImpl implements BitmapRPC.Iface{
    private EventCache cache;

    @Override
    public Map<String, Integer> initial(String tableName, Map<String, List<String>> ipMap) {
        long startTime = System.currentTimeMillis();

        FullScan fullscan = new FullScan(tableName);
        cache = fullscan.scanBasedVarName(ipMap);
        Map<String, Integer> ans = cache.getCardinality();

        long endTime = System.currentTimeMillis();
        System.out.println("scan cost: " + (endTime - startTime) + "ms");

        return ans;
    }

    @Override
    public ByteBuffer getReplayIntervals(String varName, long window, int headTailMarker) {
        long startTime = System.currentTimeMillis();

        ByteBuffer buffer = cache.generateIntervalBitmap(varName, window, headTailMarker);

        long endTime = System.currentTimeMillis();
        System.out.println("generate initial bitmap cost: " + (endTime - startTime) + "ms");

        return buffer;
    }

    @Override
    public ByteBuffer windowFilter(String variableName, long window, int headTailMarker, ByteBuffer intervalBitmap) {
        long startTime = System.currentTimeMillis();

        //RoaringBitmap bitmap = new RoaringBitmap();
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        // here we need to copy, otherwise it will have bugs
        int remaining = intervalBitmap.remaining();
        byte[] content = new byte[remaining];
        intervalBitmap.get(content);
        ByteBuffer buffer = ByteBuffer.wrap(content);
        try{
            bitmap.deserialize(buffer);
        }catch (Exception e){
            e.printStackTrace();
        }

        ByteBuffer updatedBitmap = cache.updatePointers(variableName, window, headTailMarker, bitmap);

        long endTime = System.currentTimeMillis();
        System.out.println("varName: " + variableName + " filter cost: " + (endTime - startTime) + "ms");


        return updatedBitmap;
    }

    @Override
    public ByteBuffer getAllFilteredEvents(long window, ByteBuffer intervalBitmap) {
        //RoaringBitmap bitmap = new RoaringBitmap();
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        // here we need to copy, otherwise it will have bugs
        int remaining = intervalBitmap.remaining();
        byte[] content = new byte[remaining];
        intervalBitmap.get(content);
        ByteBuffer buffer = ByteBuffer.wrap(content);
        try{
            bitmap.deserialize(buffer);
        }catch (Exception e){
            e.printStackTrace();
        }
        return cache.getRecords(window, bitmap);
    }
}
