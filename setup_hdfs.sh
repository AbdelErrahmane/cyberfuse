#!/bin/bash

# Create the /data directory in HDFS
docker exec -it hadoop-datanode hdfs dfs -mkdir -p /data

echo "Folder created in HDFS"
sleep 5

echo "------------------- Puting files in HDFS started -------------------"
# Copy files from /shared to /data in HDFS
docker exec -it hadoop-datanode hdfs dfs -put /shared/* /data/

echo "------------------- Puting files in HDFS finished -------------------"