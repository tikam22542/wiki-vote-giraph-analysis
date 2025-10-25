package org.apache.giraph.examples;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ArrayWritable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class TriangleCountComputation extends BasicComputation<
    LongWritable, IntWritable, NullWritable, ArrayWritable> {
    
    @Override
    public void compute(
        Vertex<LongWritable, IntWritable, NullWritable> vertex,
        Iterable<ArrayWritable> messages) throws IOException {
        
        if (getSuperstep() == 0) {
            Set<Long> neighbors = new HashSet<>();
            for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
                neighbors.add(edge.getTargetVertexId().get());
            }
            
            LongWritable[] neighborArray = new LongWritable[neighbors.size()];
            int i = 0;
            for (Long neighbor : neighbors) {
                neighborArray[i++] = new LongWritable(neighbor);
            }
            
            ArrayWritable neighborList = new ArrayWritable(LongWritable.class, neighborArray);
            
            for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
                sendMessage(edge.getTargetVertexId(), neighborList);
            }
            
        } else if (getSuperstep() == 1) {
            Set<Long> myNeighbors = new HashSet<>();
            for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
                myNeighbors.add(edge.getTargetVertexId().get());
            }
            
            int triangleCount = 0;
            
            for (ArrayWritable message : messages) {
                for (LongWritable neighborId : (LongWritable[]) message.toArray()) {
                    if (myNeighbors.contains(neighborId.get())) {
                        triangleCount++;
                    }
                }
            }
            
            vertex.setValue(new IntWritable(triangleCount));
        }
        
        vertex.voteToHalt();
    }
}
