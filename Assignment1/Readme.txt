how to run the jar in the cluster env:
1. start the node master,slave01,slave02
2. copy jar file and input files to docker instance:
   docker cp task1.jar master:/tmp
   docker cp task2.jar master:/tmp
   docker cp data.csv  master:/tmp
   docker cp stopwords.txt  master:/tmp
   docker cp task1-input1.txt  master:/tmp
   docker cp task1-input2.txt  master:/tmp
3. move the data file to the hdfs file
   bin/hdfs dfs -put /tmp/task1-input1.txt /
   bin/hdfs dfs -put /tmp/task1-input2.txt /
   bin/hdfs dfs -put /tmp/stopwords.txt /
   bin/hdfs dfs -put /tmp/data.csv /tmp/
4. strat the jar using given command:
   bin/hadoop jar /tmp/task1.jar  hdfs://master:9000/task1-input1.txt hdfs://master:9000/task1-output1 hdfs://master:9000/task1-input2.txt hdfs://master:9000/task1-output2 hdfs://master:9000/task1-common hdfs://master:9000/task1-result
   bin/hadoop jar /tmp/task2.jar task2.Recommend
5. check the result:
   bin/hdfs dfs -cat /task1-result/part-r-00000
   bin/hdfs dfs -cat /tmp/recommend/step5/part-r-00000

   
   
   
