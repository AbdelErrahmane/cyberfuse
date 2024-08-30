FROM bde2020/hadoop-namenode:2.0.0-hadoop3.1.3-java8

COPY copy_to_hdfs.sh /copy_to_hdfs.sh

COPY ./data /data

RUN chmod +x /copy_to_hdfs.sh

ENV HADOOP_HOME=$HADOOP_PREFIX

# CMD ["/copy_to_hdfs.sh"]