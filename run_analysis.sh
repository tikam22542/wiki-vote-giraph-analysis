#!/bin/bash

# Setup environment
export HADOOP_HOME=/opt/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

echo "========================================="
echo "Wikipedia Vote Network - Giraph Analysis"
echo "========================================="

# Download dataset
if [ ! -f "wiki-Vote.txt" ]; then
    echo "Downloading dataset..."
    wget https://snap.stanford.edu/data/wiki-Vote.txt.gz
    gunzip wiki-Vote.txt.gz
fi

# Convert to adjacency list
echo "Converting to adjacency list format..."
python3 convert_to_adjacency.py

# Build Java project
echo "Building Giraph computations..."
mvn clean package

# Set jar path
GIRAPH_JAR="target/wiki-vote-analysis-1.0-SNAPSHOT-jar-with-dependencies.jar"

# Setup local directories (pseudo-distributed mode)
mkdir -p input output

# Copy input file
cp wiki-Vote-adjacency.txt input/

echo ""
echo "========================================="
echo "Running Giraph Computations"
echo "========================================="

# Run WCC
echo ""
echo "1. Computing Weakly Connected Components..."
START=$(date +%s)
hadoop jar $GIRAPH_JAR org.apache.giraph.GiraphRunner \
    org.apache.giraph.examples.WCCComputation \
    -vif org.apache.giraph.io.formats.IntIntNullTextInputFormat \
    -vip input/wiki-Vote-adjacency.txt \
    -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat \
    -op output/wcc \
    -w 1 2>&1 | tee wcc.log
END=$(date +%s)
WCC_TIME=$((END-START))

echo ""
echo "========================================="
echo "RESULTS"
echo "========================================="
echo "WCC completed in: ${WCC_TIME}s"
echo ""
echo "Output saved to: output/wcc/"
echo "========================================="
