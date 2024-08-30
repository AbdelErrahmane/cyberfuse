#!/bin/bash

# Create the /data directory in HDFS
docker exec -it hadoop-datanode hdfs dfs -mkdir -p /data

# Copy files from /shared to /data in HDFS
docker exec -it hadoop-datanode hdfs dfs -put /shared/* /data/
