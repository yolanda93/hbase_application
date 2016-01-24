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

Script parameters:

* mode: integer whose value can be: 
     * 1: run first query
     * 2: run second query
 	 * 3: run third query
	 * 4: load data files
 * startTS: timestamp in milliseconds to be used as start timestamp.
 *  endTS: timestamp in milliseconds to be used as end timestamp.
 *  N: size of the ranking for the top-N.
 * Languages: one language or a cvs list of languages.
 *  dataFolder: path to the folder containing the files with the trending topics (the path is related to the filesystem of the node that will be used to run the HBase app). File names are lang.out, for example en.out, it.out, es.out...
 * outputFolder: path to the folder where to store the files with the query results.

  
### Usage example:
```
* Load: ./hbaseApp.sh mode dataFolder
      Ex: $ ./hbaseApp.sh 4  /home/masteruser1/logs 
* Query1: ./hbaseApp.sh mode startTS endTS N language outputFolder
      Ex: $ ./hbaseApp.sh 1  1452880040000 1452880110000 9 es /home/masteruser1
* Query2: ./hbaseApp.sh mode startTS endTS N language outputFolder
      Ex: $ ./hbaseApp.sh 2  1452880040000 1452880110000 6 es,en,it /home/yolanda 
* Query3: ./hbaseApp.sh mode startTS endTS N outputFolder
      Ex: $./hbaseApp.sh 3  1452880040000 1452880110000 5 /home/yolanda

```
### Steps:

0. Generate the files lang.out
1. hbase-site.xml with the property hbase.zookeeper.quorum: it must point to ZK instance of the mini cluster assigned to your group
2. Compile the project and generate hbaseApp.sh script:
	mvn clean compile package appassembler:assemble

## Example of execution on a cluster

1) Connection to the cluster 
```
ssh masteruser1@138.4.110.141 -p 51002 --> storm H2
ssh masteruser1@138.4.110.141 -p 51001 --> Hadoop and Hbase H1
```
2) Copy files
```
scp -P 51001 -r appassembler masteruser1@138.4.110.141:/home/masteruser1
```
3) Zookeeper on H2 must be up and running
```
./zookeeper-3.4.6/bin/zkServer.sh start
./zookeeper-3.4.6/bin/zkServer.sh stop
```
4) Run haddoop
```
Hadoop start/stop from H1: 
 Start:  ./hadoop-2.5.2/sbin/start-dfs.sh
 Stop:   ./hadoop-2.5.2/sbin/stop-dfs.sh
```
5) Run Hbase
```
start/stop from H1: 
Start:  ./hbase-0.98.16.1-hadoop2/bin/start-hbase.sh
Stop:   ./hbase-0.98.16.1-hadoop2/bin/stop-hbase.sh

HBase UI
http://138.4.110.141:60010/

Start the shell: bin/hbase shell
```

6) If you have a zookeeper already running, it is required to stop it first.
```
sudo service zookeeper stop
```
## Contact information
		
Yolanda de la Hoz Simón. yolanda93h@gmail.com