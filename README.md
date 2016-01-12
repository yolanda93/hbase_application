# HBase application 
   
The goal of this project is to implement a Java application that stores trending topics from Twitter into HBase and provides users with a set of queries for data analysis.

The trending topics to load in HBase are stored into text files with the same format used to store the results of the project 1 assignment.
This format is: 1 file per language and each line of the file with the CSV format “timestamp_ms, lang, tophashtag1, frequencyhashtag1, tophashtag2, frequencyhashtag2, tophashtag3, frequencyhashtag3”.

The query set is composed by 3 queries:

	1. Given a language (lang), do find the Top-N most used words for the given language in
	a time interval defined with a start and end timestamp. Start and end timestamp are
	in milliseconds.

	2. Do find the list of Top-N most used words for each language in a time interval defined
	with the provided start and end timestamp. Start and end timestamp are in
	milliseconds.

	3. Do find the Top-N most used words and the frequency of each word regardless the
	language in a time interval defined with the provided start and end timestamp. Start
	and end timestamp are in milliseconds.

## HBaseApp.sh

### Usage:

Arguments:

Script parameters:

	o Mode, integer whose value can be:
		o 1: run first query
		o 2: run second query
		o 3: run third query
		o 4: load data files
	o startTS: timestamp in milliseconds to be used as start timestamp.
	o endTS: timestamp in milliseconds to be used as end timestamp.
	o N: size of the ranking for the top-N.
	o Languages: one language or a cvs list of languages.
	o dataFolder: path to the folder containing the files with the trending topics (the path is related to the filesystem of the node that will be used to run the HBase app). File names are lang.out, for example en.out, it.out, es.out...
	o outputFolder: path to the folder where to store the files with the query results.
  

Usage example:

   o Load: ./hbaseApp.sh mode dataFolder
      Ex:./hbaseApp.sh 4 /local/data
   o Query1: ./hbaseApp.sh mode startTS endTS N language outputFolder
      Ex:./hbaseApp.sh 1 1450714465000 1450724465000 7 en /local/output/
   o Query2: ./hbaseApp.sh mode startTS endTS N language outputFolder
      Ex:./hbaseApp.sh 2 1450714465000 1450724465000 5 en,it,es /local/output/
   o Query3: ./hbaseApp.sh mode startTS endTS N outputFolder
      Ex:./hbaseApp.sh 3 1450714465000 1450724465000 10 /local/output/

```
## Contact information

Yolanda de la Hoz Simón. yolanda93h@gmail.com


Steps:

0. Generate the files lang.out
1. hbase-site.xml with the property hbase.zookeeper.quorum: it must point to ZK instance of the mini cluster assigned to your group
2. Load in hbase files

Store the result of the query in a file 

Compile the project and generate hbaseApp.sh script:

mvn clean compile package appassembler:assemble

## Example of execution on a cluster

1) Connection to the cluster 
username: masteruser1
password: 7Uljjbpb4

Copy the files
scp -P 51001 -r appassembler masteruser1@138.4.110.141:/home/masteruser1

Zookeeper on H2 must be up and running

ssh masteruser1@138.4.110.141 -p 51002 --> storm H2	

./zookeeper-3.4.6/bin/zkServer.sh start
./zookeeper-3.4.6/bin/zkServer.sh stop

ssh masteruser1@138.4.110.141 -p 51001 --> Hadoop and Hbase H1	

Hadoop start/stop from H1: 
 Start:  ./hadoop-2.5.2/sbin/start-dfs.sh
 Stop:   ./hadoop-2.5.2/sbin/stop-dfs.sh

Hbase
start/stop from H1: 

Start:  ./hbase-0.98.16.1-hadoop2/bin/start-hbase.sh
Stop:   ./hbase-0.98.16.1-hadoop2/bin/stop-hbase.sh

1) Run hbase 

Start HBase: bin/start-hbase.sh
Start the shell: bin/hbase shell