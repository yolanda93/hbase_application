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

   Load: ./hbaseApp.sh mode dataFolder
    o Ex:./hbaseApp.sh 4 /local/data
   Query1: ./hbaseApp.sh mode startTS endTS N language outputFolder
    o Ex:./hbaseApp.sh 1 1450714465000 1450724465000 7 en /local/output/
   Query2: ./hbaseApp.sh mode startTS endTS N language outputFolder
    o Ex:./hbaseApp.sh 2 1450714465000 1450724465000 5 en,it,es /local/output/
   Query3: ./hbaseApp.sh mode startTS endTS N outputFolder
    o Ex:./hbaseApp.sh 3 1450714465000 1450724465000 10 /local/output/

```
## Contact information

Yolanda de la Hoz Simón. yolanda93h@gmail.com