package org.apache.giraph.examples;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;

/**
 * Computes local clustering coefficient for each vertex.
 * Coefficient = 2 * triangles / (degree * (degree - 1))
 */
public class ClusteringCoefficientComputation extends BasicComputation<
    LongWritable, DoubleWritable, NullWritable, IntWritable> {
    
    @Override
    public void compute(
        Vertex<LongWritable, DoubleWritable, NullWritable> vertex,
        Iterable<IntWritable> messages) throws IOException {
        
        int degree = vertex.getNumEdges();
        
        if (degree < 2) {
            vertex.setValue(new DoubleWritable(0.0));
            vertex.voteToHalt();
            return;
        }
        
        // Assume triangle count was computed and passed as message
        int triangleCount = 0;
        for (IntWritable message : messages) {
            triangleCount = message.get();
            break;
        }
        
        // Compute clustering coefficient
        double coefficient = (2.0 * triangleCount) / (degree * (degree - 1));
        vertex.setValue(new DoubleWritable(coefficient));
        
        vertex.voteToHalt();
    }
}
