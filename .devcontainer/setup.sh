#!/bin/bash

echo "Installing dependencies..."
apt-get update
apt-get install -y wget curl maven ssh openssh-server

echo "Installing Hadoop..."
cd /opt
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz
ln -s hadoop-3.3.6 hadoop
rm hadoop-3.3.6.tar.gz

echo "Setting environment variables..."
echo 'export HADOOP_HOME=/opt/hadoop' >> ~/.bashrc
echo 'export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin' >> ~/.bashrc
echo 'export JAVA_HOME=/usr/local/sdkman/candidates/java/current' >> ~/.bashrc

echo "Configuring Hadoop for standalone mode..."
mkdir -p /opt/hadoop/input

echo "Installing Giraph..."
cd /workspace
wget https://repo1.maven.org/maven2/org/apache/giraph/giraph-examples/1.2.0/giraph-examples-1.2.0-for-hadoop-2.5.1-jar-with-dependencies.jar

echo "Setup complete!"
