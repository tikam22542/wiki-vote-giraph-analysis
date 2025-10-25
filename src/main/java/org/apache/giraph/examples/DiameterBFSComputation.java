package org.apache.giraph.examples;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import java.io.IOException;

/**
 * BFS from a source vertex to compute distances.
 * Run multiple times from different sources to estimate diameter.
 */
public class DiameterBFSComputation extends BasicComputation<
    LongWritable, IntWritable, NullWritable, IntWritable> {
    
    private static final int SOURCE_VERTEX = 1; // Set source vertex ID
    private static final int INFINITY = Integer.MAX_VALUE;
    
    @Override
    public void compute(
        Vertex<LongWritable, IntWritable, NullWritable> vertex,
        Iterable<IntWritable> messages) throws IOException {
        
        int currentDistance = vertex.getValue().get();
        
        // Initialize distances
        if (getSuperstep() == 0) {
            if (vertex.getId().get() == SOURCE_VERTEX) {
                vertex.setValue(new IntWritable(0));
                sendMessageToAllEdges(vertex, new IntWritable(1));
            } else {
                vertex.setValue(new IntWritable(INFINITY));
            }
            return;
        }
        
        // Update distance if shorter path found
        boolean changed = false;
        for (IntWritable message : messages) {
            int newDistance = message.get();
            if (newDistance < currentDistance) {
                currentDistance = newDistance;
                changed = true;
            }
        }
        
        // Propagate distance
        if (changed) {
            vertex.setValue(new IntWritable(currentDistance));
            sendMessageToAllEdges(vertex, new IntWritable(currentDistance + 1));
        }
        
        vertex.voteToHalt();
    }
}
