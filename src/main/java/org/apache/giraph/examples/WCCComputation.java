package org.apache.giraph.examples;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import java.io.IOException;

public class WCCComputation extends BasicComputation<
    LongWritable, LongWritable, NullWritable, LongWritable> {
    
    @Override
    public void compute(
        Vertex<LongWritable, LongWritable, NullWritable> vertex,
        Iterable<LongWritable> messages) throws IOException {
        
        long currentComponent = vertex.getValue().get();
        boolean changed = false;
        
        if (getSuperstep() == 0) {
            vertex.setValue(new LongWritable(vertex.getId().get()));
            sendMessageToAllEdges(vertex, vertex.getValue());
            return;
        }
        
        for (LongWritable message : messages) {
            long msgValue = message.get();
            if (msgValue < currentComponent) {
                currentComponent = msgValue;
                changed = true;
            }
        }
        
        if (changed) {
            vertex.setValue(new LongWritable(currentComponent));
            sendMessageToAllEdges(vertex, vertex.getValue());
        }
        
        vertex.voteToHalt();
    }
}
