#!/bin/bash

export HADOOP_HOME=/opt/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

echo "========================================="
echo "WIKIPEDIA VOTE NETWORK - GIRAPH ANALYSIS"
echo "========================================="

# Download and prepare data
if [ ! -f "wiki-Vote.txt" ]; then
    echo "Downloading dataset..."
    wget https://snap.stanford.edu/data/wiki-Vote.txt.gz
    gunzip wiki-Vote.txt.gz
fi

echo "Converting to adjacency list..."
python3 convert_to_adjacency.py

echo "Building project..."
mvn clean package

GIRAPH_JAR="target/wiki-vote-analysis-1.0-SNAPSHOT-jar-with-dependencies.jar"
mkdir -p input output

cp wiki-Vote-adjacency.txt input/

echo ""
echo "========================================="
echo "RUNNING ALL METRICS"
echo "========================================="

# 1. WCC
echo ""
echo "[1/5] Weakly Connected Components..."
START=$(date +%s)
hadoop jar $GIRAPH_JAR org.apache.giraph.GiraphRunner \
    org.apache.giraph.examples.WCCComputation \
    -vif org.apache.giraph.io.formats.IntIntNullTextInputFormat \
    -vip input/wiki-Vote-adjacency.txt \
    -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat \
    -op output/wcc \
    -w 1 2>&1 | tee logs/wcc.log
END=$(date +%s)
WCC_TIME=$((END-START))
echo "✓ WCC: ${WCC_TIME}s"

# 2. SCC
echo ""
echo "[2/5] Strongly Connected Components..."
START=$(date +%s)
hadoop jar $GIRAPH_JAR org.apache.giraph.GiraphRunner \
    org.apache.giraph.examples.SCCForwardComputation \
    -vif org.apache.giraph.io.formats.IntIntNullTextInputFormat \
    -vip input/wiki-Vote-adjacency.txt \
    -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat \
    -op output/scc \
    -w 1 2>&1 | tee logs/scc.log
END=$(date +%s)
SCC_TIME=$((END-START))
echo "✓ SCC: ${SCC_TIME}s"

# 3. Triangle Count
echo ""
echo "[3/5] Triangle Count..."
START=$(date +%s)
hadoop jar $GIRAPH_JAR org.apache.giraph.GiraphRunner \
    org.apache.giraph.examples.TriangleCountComputation \
    -vif org.apache.giraph.io.formats.IntIntNullTextInputFormat \
    -vip input/wiki-Vote-adjacency.txt \
    -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat \
    -op output/triangles \
    -w 1 2>&1 | tee logs/triangles.log
END=$(date +%s)
TRIANGLE_TIME=$((END-START))
echo "✓ Triangles: ${TRIANGLE_TIME}s"

# 4. Clustering Coefficient
echo ""
echo "[4/5] Clustering Coefficient..."
START=$(date +%s)
hadoop jar $GIRAPH_JAR org.apache.giraph.GiraphRunner \
    org.apache.giraph.examples.ClusteringCoefficientComputation \
    -vif org.apache.giraph.io.formats.IntIntNullTextInputFormat \
    -vip input/wiki-Vote-adjacency.txt \
    -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat \
    -op output/clustering \
    -w 1 2>&1 | tee logs/clustering.log
END=$(date +%s)
CLUSTERING_TIME=$((END-START))
echo "✓ Clustering: ${CLUSTERING_TIME}s"

# 5. Diameter
echo ""
echo "[5/5] Diameter (BFS)..."
START=$(date +%s)
hadoop jar $GIRAPH_JAR org.apache.giraph.GiraphRunner \
    org.apache.giraph.examples.DiameterBFSComputation \
    -vif org.apache.giraph.io.formats.IntIntNullTextInputFormat \
    -vip input/wiki-Vote-adjacency.txt \
    -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat \
    -op output/diameter \
    -w 1 2>&1 | tee logs/diameter.log
END=$(date +%s)
DIAMETER_TIME=$((END-START))
echo "✓ Diameter: ${DIAMETER_TIME}s"

TOTAL_TIME=$((WCC_TIME + SCC_TIME + TRIANGLE_TIME + CLUSTERING_TIME + DIAMETER_TIME))

echo ""
echo "========================================="
echo "GIRAPH EXECUTION TIME SUMMARY"
echo "========================================="
echo "WCC:                  ${WCC_TIME}s"
echo "SCC:                  ${SCC_TIME}s"
echo "Triangle Count:       ${TRIANGLE_TIME}s"
echo "Clustering Coeff:     ${CLUSTERING_TIME}s"
echo "Diameter:             ${DIAMETER_TIME}s"
echo "----------------------------------------"
echo "TOTAL:                ${TOTAL_TIME}s"
echo "========================================="

# Save results
echo "WCC,${WCC_TIME}" > giraph_times.csv
echo "SCC,${SCC_TIME}" >> giraph_times.csv
echo "Triangle Count,${TRIANGLE_TIME}" >> giraph_times.csv
echo "Clustering Coefficient,${CLUSTERING_TIME}" >> giraph_times.csv
echo "Diameter,${DIAMETER_TIME}" >> giraph_times.csv
echo "TOTAL,${TOTAL_TIME}" >> giraph_times.csv

echo ""
echo "Results saved to giraph_times.csv"
