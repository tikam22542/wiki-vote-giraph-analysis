package org.apache.giraph.examples;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Writable;
import java.io.IOException;

public class SCCForwardComputation extends BasicComputation<
    LongWritable, MapWritable, NullWritable, LongWritable> {
    
    @Override
    public void compute(
        Vertex<LongWritable, MapWritable, NullWritable> vertex,
        Iterable<LongWritable> messages) throws IOException {
        
        MapWritable reachableSet = vertex.getValue();
        if (reachableSet == null) {
            reachableSet = new MapWritable();
        }
        boolean changed = false;
        
        if (getSuperstep() == 0) {
            reachableSet.put(vertex.getId(), new BooleanWritable(true));
            vertex.setValue(reachableSet);
            sendMessageToAllEdges(vertex, vertex.getId());
            return;
        }
        
        for (LongWritable message : messages) {
            if (!reachableSet.containsKey(message)) {
                reachableSet.put(new LongWritable(message.get()), new BooleanWritable(true));
                changed = true;
            }
        }
        
        if (changed) {
            vertex.setValue(reachableSet);
            for (Writable key : reachableSet.keySet()) {
                sendMessageToAllEdges(vertex, (LongWritable) key);
            }
        }
        
        vertex.voteToHalt();
    }
}
