#!/bin/bash

# Wait for HDFS to be ready
until hdfs dfs -ls /; do
  echo "Waiting for HDFS..."
  sleep 5
done

echo "Run copying."

# Create directory in HDFS (if not exists)
hdfs dfs -mkdir  /data
# Copy files from local to HDFS
hdfs dfs -put /data/* /data/


echo "Files copied to HDFS successfully."