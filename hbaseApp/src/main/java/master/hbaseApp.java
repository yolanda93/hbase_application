package master;

import java.io.BufferedReader;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.Collator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author Yolanda de la Hoz Simon - 53826071E
 * @version 1.2 2 de Enero de 2016
 */
public class hbaseApp {
    private static final String ID = "10";
	private HBaseAdmin admin;
	private HTable table;
	private String current_table;
	private String[] all_languages;
	private String[] query_languages;
	private Map<String, Long> intervalTopTopic;
	
	/**
	 * Method to generate the structure of the key  
	 */
	private byte[] generateKey(String timestamp) {
		byte[] key = new byte[44];
		System.arraycopy(Bytes.toBytes(timestamp),0,key,0,timestamp.length());
		return key;
	}
	
	/**
	 * Method to generate the structure of the key
	 */
	private byte[] generateKey(String timestamp, String lang,String topic_pos) {
		byte[] key = new byte[44];
		System.arraycopy(Bytes.toBytes(timestamp),0,key,0,timestamp.length());
		System.arraycopy(Bytes.toBytes(lang),0,key,20,lang.length());
		System.arraycopy(Bytes.toBytes(topic_pos),0,key,20,topic_pos.length());
		return key;
	}
	
	/**
	 * Method to arrange lexicographically and rank results.
	 */
	private List<Entry<String, Long>> arrangeMap(Map<String, Long> map) {
		Set<Entry<String, Long>> set = map.entrySet();
		List<Entry<String, Long>> list = new ArrayList<Entry<String, Long>>(set);

		// lexicographically order
		Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
			Collator c = Collator.getInstance();
			public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
				if(c.compare(o2.getKey(), o1.getKey())==-1)
					return 1;
				else if(c.compare(o2.getKey(), o1.getKey())==1)
					return -1;
				else
					return 0;
			}
		});
	
     Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
         public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {            	
         	return (o2.getValue()).compareTo(o1.getValue());
         }
     });
     return list;
	}
     
	/**
	 * Method to arrange and print the query result
	 */
	private void arrangeAndPrint(Map<String, Long> intervalTopTopic, String query,String lang, String start_timestamp,  String end_timestamp, String  output_folder,int N) {
		// Process the results and print them
		List<Entry<String, Long>> intervalTopTopicList = this.arrangeMap(intervalTopTopic);
		int position = 1;
		//System.out.println("The length is : " + intervalTopTopicList.size());
		for (Map.Entry<String, Long> entry : intervalTopTopicList) {				            	
			//System.out.println("The result for the first query is:" + "TOPIC: " + entry.getKey() +  " Position: " + position + "Count" + entry.getValue());
			writeInOutputFile(query,lang, position,entry.getKey(),start_timestamp,end_timestamp,output_folder,entry.getValue().toString());	    	
			if (position == N) 	
				break;
			else 
				position++;
		}
	}
	
	private void executeQuery(String query,String start_timestamp, String end_timestamp,int N,String lang, String out_folder_path) {
		System.out.println("Executing the " + query);
	    
		Scan scan = new Scan(generateKey(start_timestamp),generateKey(end_timestamp));	
	    scan.addFamily(Bytes.toBytes(lang));
		//System.out.println("Get the results");
		ResultScanner rs;
		try {
			rs = table.getScanner(scan);	
			Result res = rs.next();
			if(!query.equals("query3"))
			intervalTopTopic = new HashMap<String,Long>();
			while (res!=null && !res.isEmpty()){
						byte [] topic_bytes = res.getValue(Bytes.toBytes(lang),Bytes.toBytes("TOPIC"));
						byte [] count_bytes = res.getValue(Bytes.toBytes(lang),Bytes.toBytes("COUNTS"));
						String topic = Bytes.toString(topic_bytes).toString();
						String count = Bytes.toString(count_bytes).toString();				
						intervalTopTopic.put(topic, (long) Integer.parseInt(count));
				res = rs.next();
			}
		if(!query.equals("query3"))
		 arrangeAndPrint(intervalTopTopic,query,lang,start_timestamp,end_timestamp,out_folder_path,N);
		
		}catch (IOException e) { 	
			e.printStackTrace();
		}
	}
	
	/**
	 * Method to perform the first query
	 * Given a language (lang), do find the Top-N most used words for the given language in
	 * a time interval defined with a start and end timestamp. Start and end timestamp are
	 * in milliseconds.      
	 */
	private void firstQuery(String start_timestamp, String end_timestamp,int N,String language, String outputFolderPath) {	 
		executeQuery("query1",start_timestamp,end_timestamp,N,language,outputFolderPath);
	}

	/**
	 * Method to perform the second query
	 * Do find the list of Top-N most used words for each language in a time interval defined
     * with the provided start and end timestamp. Start and end timestamp are in milliseconds. 
	 */
	private void secondQuery(String start_timestamp, String end_timestamp,int N,String[] languages, String outputFolderPath) {	 
		for (int i = 0; i <= languages.length - 1; i++) {
			try {
				if(table.getTableDescriptor().hasFamily(Bytes.toBytes(languages[i]))){
					executeQuery("query2",start_timestamp,end_timestamp,N,languages[i],outputFolderPath);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}  
		
	}

	/**
	 * Method to perform the third query
	 * Do find the Top-N most used words and the frequency of each word regardless the
     * language in a time interval defined with the provided start and end timestamp. Start
     * and end timestamp are in milliseconds.      	
	 */
	private void thirdQuery(String start_timestamp, String end_timestamp,int N,String outputFolderPath) {		
		//System.out.println("Executing the query3");	
		intervalTopTopic = new HashMap<String,Long>();
			try {
				query_languages= new String[table.getTableDescriptor().getColumnFamilies().length];
				for (int i = 0; i <= table.getTableDescriptor().getColumnFamilies().length - 1; i++) {
					query_languages[i]=  table.getTableDescriptor().getColumnFamilies()[i].getNameAsString();
					executeQuery("query3",start_timestamp,end_timestamp,N,query_languages[i],outputFolderPath);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}	
		arrangeAndPrint(intervalTopTopic,"query3",null,start_timestamp,end_timestamp,outputFolderPath,N);
	}
	
	/**
	 * Method to extract languages from the data source 
	 */
	private String[] extractLangsSource(String dataFolder) {
		File folder = new File(dataFolder);
		File[] listOfFiles = folder.listFiles();
		String [] langs = new String[listOfFiles.length];
		for (int i = 0; i < listOfFiles.length; i++) {
		 File file = listOfFiles[i];
			if (file.isFile() && file.getName().endsWith(".out")) {
				langs[i]=file.getName().split(".out")[0].toString();
			}
		}
		return langs;
	}	
	
	/**
	 * Method to create the table in hbase
	 */
	private void getTable() {
		System.setProperty("hadoop.home.dir", "/");
		Configuration conf = HBaseConfiguration.create(); // Instantiating configuration class
		conf.set("hbase.zookeeper.quorum", "node2"); 
		//conf.addResource(new Path("/home/masteruser1/hbase-0.98.16.1-hadoop2/conf/hbase-site.xml"));
		try {
			admin = new HBaseAdmin(conf);
			if(!admin.tableExists(current_table)){// Execute the table through admin	
				//System.out.println("Creating table in hbase");
				// Instantiating table descriptor class
				HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(current_table));
				// Adding column families to table descriptor
				for (int i = 0; i < all_languages.length; i++) {
				  tableDescriptor.addFamily(new HColumnDescriptor(all_languages[i]));
				}
				admin.createTable(tableDescriptor);	
				HConnection conn = HConnectionManager.createConnection(conf);
				table = new HTable(TableName.valueOf(current_table),conn);
				//System.out.println("Table created: "  + table.getName());
			 }else{		
				 HConnection conn = HConnectionManager.createConnection(conf);
				 table = new HTable(TableName.valueOf(current_table),conn);
				 //System.out.println("Table opened: " + table.getName());
			 }
		} catch (IOException e) {
			e.printStackTrace();
		}
	}	

	/**
	 * Method to insert rows into the hbase table
	 */
	private void insertIntoTable(String timestamp, String lang,String hashtag, String counts,int topic_pos) {
		//System.out.println("Inserting into table with key: " + timestamp + ", " + lang + ", " + topic_pos);	
		byte[] key = generateKey(timestamp,lang,Integer.toString(topic_pos));
		Get get = new Get(key);
		Result res;
		try {
			res = table.get(get);
			if(res != null){ // insert in table
				Put put = new Put(key);
				put.add(Bytes.toBytes(lang),Bytes.toBytes("TOPIC"),Bytes.toBytes(hashtag));
				put.add(Bytes.toBytes(lang),Bytes.toBytes("COUNTS"),Bytes.toBytes(counts));
				table.put(put);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}
	
	/**
	 * Method to load the files in Hbase
	 */
	private void load(String dataFolder) {
		//System.out.println("Loading data into hbase");	
		File folder = new File(dataFolder);
		File[] listOfFiles = folder.listFiles();
		//System.out.println("Number of files: " + listOfFiles.length);	
		for (int i = 0; i < listOfFiles.length; i++) {
			File file = listOfFiles[i];
			//System.out.println("Reading the file: " + file.getName());	
			if (file.isFile() && file.getName().endsWith(".out")) {
			
				try(BufferedReader br = new BufferedReader(new FileReader(file))) {
					for(String line; (line = br.readLine()) != null; ) {				
						// process line by line
						String[] fields = line.split(",");
						//System.out.println(line);						
						String timestamp = fields[0];
						String lang = fields[1];
						int pos=2;
						int topic_pos=1;
						while(pos<fields.length){
							insertIntoTable(timestamp,lang,fields[pos++],fields[pos++],topic_pos);
							topic_pos++;
						}
					}
			    //System.out.println("Data sucessfully loaded");	
				} catch (NumberFormatException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}	
			} 
		}

	}

	/**
	 * Method to store the results of the query in a file  
	 */
	private void writeInOutputFile(String query, String language, int position, String word, String startTS,String endTS, String out_folder_path, String frecuency) {
        File file = new File(out_folder_path + "/" + ID + "_" + query + ".out");
        String content;
        if(query.equals("query3"))
           content= position + ", " + word + ", " + frecuency + ", " + startTS + ", " + endTS;
        else
           content= language + ", " + position + ", " + word + ", " + startTS + ", " + endTS;
        
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(file, true));
            bw.append(content);
            bw.newLine();
            bw.close();
        } catch (FileNotFoundException ex) {
            System.out.println(ex.getMessage());
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
       // System.out.println("Write done");	
	}

	/**
	 * Method to start the hbase app with the selected query
	 * @param mode Mode to start the app. Mode 1 reads from file. Mode 2 reads from twitter API.     
	 */
	private void start(String[] args,int mode) {
		current_table="TopTopic1";
		if(mode==4)
		   all_languages=extractLangsSource(args[1]);
		
	    getTable();
		switch (mode) {
		case 1: 	firstQuery(args[1],args[2],Integer.parseInt(args[3]),args[4],args[5]);
		break;
		case 2: 	secondQuery(args[1],args[2],Integer.parseInt(args[3]),args[4].split(","),args[5]);
		break;
		case 3: 	thirdQuery(args[1],args[2],Integer.parseInt(args[3]),args[4]);
		break;
		case 4: 	load(args[1]);            		            
		break;     	
		}
	}

	/**
	 * Main method
	 * @param args Arguments: mode dataFolder startTS endTS N language outputFolder    
	 * @throws java.lang.Exception    
	 */
	public static void main(String[] args) throws Exception {
		org.apache.log4j.BasicConfigurator.configure();
		int mode = 0;
		if (args.length > 0) {
			System.out.println("Started hbaseApp with mode: " + args[0]);
			try {
				mode = Integer.parseInt(args[0]);
				if (mode==4 && args.length<2 ) {
					System.out.println("To start the App with mode 4 it is required the mode and the dataFolder");
					System.exit(1);  
				}
				if (mode==2 && args.length!=6 ) {
					System.out.println("To start the App with mode 2 it is required the mode startTS endTS N language outputFolder");
					System.exit(1);  
				}
				if (mode==3 && args.length!=5 ) {
					System.out.println("To start the App with mode 3 it is required the mode startTS endTS N language outputFolder");
					System.exit(1);  
				}     
				if (mode==1 && args.length!=6) {
					System.out.println("To start the App with mode 1 it is required the mode startTS endTS N outputFolder");
					System.exit(1);  
				}  

				hbaseApp app = new hbaseApp();
				app.start(args,mode);
				//app.admin.shutdown();

			} catch (Exception e) {
				System.out.println(e.getMessage());
			}

		} else {
			System.out.println("Arguments: mode dataFolder startTS endTS N language outputFolder");
			System.exit(1);
		}

	}

}
