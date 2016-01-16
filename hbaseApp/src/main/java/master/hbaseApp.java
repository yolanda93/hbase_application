package master;

import java.io.BufferedReader;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Hashtable;


import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author Yolanda de la Hoz Simon - 53826071E
 * @version 1.2 2 de Enero de 2016
 */
public class hbaseApp {
    private static final String ID = "53826071E";
	private int startTS;
	private int endTS;
	private int N;
	private String[] languages;
	private String outputFolderPath;
	private String dataFolder;
	private HBaseAdmin admin;
	private HTable table;

	/**
	 * Method to generate the structure of the key  
	 */
	private byte[] generateStartKey(String timestamp) {
		byte[] key = new byte[44];
		System.arraycopy(Bytes.toBytes(timestamp),0,key,0,timestamp.length());
		return key;
	}
	
	/**
	 * Method to generate the structure of the key  
	 */
	private byte[] generateEndKey(String timestamp) {
		byte[] key = new byte[44];
		System.arraycopy(Bytes.toBytes(timestamp),0,key,0,timestamp.length());
		return key;
	}
	
	/**
	 * Method to perform the first query
	 * Given a language (lang), do find the Top-N most used words for the given language in
	 * a time interval defined with a start and end timestamp. Start and end timestamp are
	 * in milliseconds.      
	 */
	private byte[] generateKey(String timestamp, String lang) {
		byte[] key = new byte[44];
		System.arraycopy(Bytes.toBytes(timestamp),0,key,0,timestamp.length());
		System.arraycopy(Bytes.toBytes(lang),0,key,20,lang.length());
		return key;
	}
	
	/**
	 * Method to perform the first query
	 * Given a language (lang), do find the Top-N most used words for the given language in
	 * a time interval defined with a start and end timestamp. Start and end timestamp are
	 * in milliseconds.      
	 */
	private void firstQuery(String start_timestamp, String end_timestamp,String N,String lang, String outputFolderPath) {
		System.out.println("Executing the first query");
		Scan scan = new Scan(generateStartKey(start_timestamp),
				generateEndKey(end_timestamp));
		System.out.println("Executing the first query");
	    Filter f = new SingleColumnValueFilter(Bytes.toBytes("hashtags"), Bytes.toBytes("LANG"),
				CompareFilter.CompareOp.EQUAL,Bytes.toBytes(lang));	
		scan.setFilter(f);
		System.out.println("dsfsdfsd");
		ResultScanner rs;
		try {
			rs = table.getScanner(scan);
			Result res = rs.next();
			while (res!=null && !res.isEmpty()){
				// Do something with the result.
				res = rs.next();
				System.out.println("The result for the first query is:" + res.toString());
			} 
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Method to perform the second query
	 * Do find the list of Top-N most used words for each language in a time interval defined
     * with the provided start and end timestamp. Start and end timestamp are in
     * milliseconds. 
	 */
	private void secondQuery(String start_timestamp, String end_timestamp,String N,String[] languages, String outputFolderPath) {
		Scan scan = new Scan(generateStartKey(start_timestamp),
				generateEndKey(end_timestamp));

		for(int i =0;i<languages.length;i++){
			Filter f = new SingleColumnValueFilter(Bytes.toBytes("hashtags"), Bytes.toBytes("LANG"),
					CompareFilter.CompareOp.EQUAL,Bytes.toBytes(languages[i]));	
			scan.setFilter(f);
		}

		ResultScanner rs;
		try {
			rs = table.getScanner(scan);
			Result res = rs.next();
			while (res!=null && !res.isEmpty()){
				// Do something with the result.
				res = rs.next();
				System.out.println("The result for the second query is:" + res.toString());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Method to perform the third query
	 * Do find the Top-N most used words and the frequency of each word regardless the
     * language in a time interval defined with the provided start and end timestamp. Start
     * and end timestamp are in milliseconds.      	
	 */
	private void thirdQuery(String start_timestamp, String end_timestamp,String N,String outputFolderPath) {
		Scan scan = new Scan(generateStartKey(start_timestamp),
				generateEndKey(end_timestamp));	

		ResultScanner rs;
		try {
			rs = table.getScanner(scan);
			
			Result res = rs.next();
			while (res!=null && !res.isEmpty()){
				// Do something with the result.
				res = rs.next();
				System.out.println("The result for the first query is:" + res.toString());
			}	
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Method to create the table in hbase
	 */
	private void createTable() {
		System.setProperty("hadoop.home.dir", "/");
		Configuration conf = HBaseConfiguration.create(); // Instantiating configuration class

		try {
			admin = new HBaseAdmin(conf);
			if(!admin.tableExists("TopTopics")){// Execute the table through admin	
				System.out.println("Creating table in hbase");
				// Instantiating table descriptor class
				HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("TopTopics"));

				// Adding column families to table descriptor
				tableDescriptor.addFamily(new HColumnDescriptor("hashtags"));
                 
				admin.createTable(tableDescriptor);				
				System.out.println(" Table created ");
			 }
			table = new HTable(conf, "TopTopics");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}	

	/**
	 * Method to insert into the hbase table
	 */
	private void insertIntoTable(String timestamp, String lang,String hashtag, String counts) {
		byte[] key = generateKey(timestamp,lang);
		Get get = new Get(key);
		Result res;
		try {
			res = table.get(get);
			if(res != null){ // insert in table
				System.out.println("res not null");
				Put put = new Put(key);
				put.add(Bytes.toBytes("hashtags"),Bytes.toBytes("TOPIC"),Bytes.toBytes(hashtag));
				put.add(Bytes.toBytes("hashtags"),Bytes.toBytes("LANG"),Bytes.toBytes(lang));
				put.add(Bytes.toBytes("hashtags"),Bytes.toBytes("COUNTS"),Bytes.toBytes(counts));
				table.put(put);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}
	
	/**
	 * Method to load the files in hbase
	 */
	private void load(String dataFolder) {
		System.out.println("Loading data into hbase");	
		File folder = new File(dataFolder);
		File[] listOfFiles = folder.listFiles();
		System.out.println(listOfFiles.length);	
		for (int i = 0; i < listOfFiles.length; i++) {
			File file = listOfFiles[i];
			System.out.println(file.getName());	
			if (file.isFile() && file.getName().endsWith(".log")) {
			
				try(BufferedReader br = new BufferedReader(new FileReader(file))) {
					for(String line; (line = br.readLine()) != null; ) {
							
						// process line by line
						String[] fields = line.split(",");
						System.out.println(line);
						int pos = 0;
						String timestamp = fields[0];
						System.out.println(timestamp);
						String lang = fields[1];
						System.out.println(lang);
						pos=2;
						while(pos<fields.length){
							insertIntoTable(timestamp,lang,fields[pos++],fields[pos++]);
						}
					}
					System.out.println("Data sucessfully loaded");	
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
	private void writeInOutputFile(String query, String language, String position, String word, String startTS,String endTS) {
        File file = new File(outputFolderPath + "/" + ID + "_" + query + ".out");
        String content = language + "," + position + "," + word + "," + startTS + "," + endTS;
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
        System.out.println("Write done");	
	}

	/**
	 * Method to start the hbase app with the selected query
	 * @param mode Mode to start the app. Mode 1 reads from file. Mode 2 reads from twitter API.     
	 */
	private void start(String[] args,int mode) {
	    createTable();
		switch (mode) {
		case 1: 	firstQuery(args[1],args[2],args[3],args[4],args[5]);
		break;
		case 2: 	secondQuery(args[1],args[2],args[3],args[4].split(","),args[5]);
		break;
		case 3: 	thirdQuery(args[1],args[2],args[3],args[4]);
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
					System.out.println("To start the App with mode 1 it is required the mode and the dataFolder");
					System.exit(1);  
				}
				if (mode==2 && args.length!=6 ) {
					System.out.println("To start the App with mode 2 it is required the mode startTS endTS N language outputFolder");
					System.exit(1);  
				}
				if (mode==3 && args.length!=6 ) {
					System.out.println("To start the App with mode 3 it is required the mode startTS endTS N language outputFolder");
					System.exit(1);  
				}     
				if (mode==1 && args.length!=6) {
					System.out.println("To start the App with mode 1 it is required the mode startTS endTS N outputFolder");
					System.exit(1);  
				}  

				hbaseApp app = new hbaseApp();
				app.start(args,mode);
				app.admin.shutdown();

			} catch (Exception e) {
				System.out.println(e.getMessage());
			}

		} else {
			System.out.println("Arguments: mode dataFolder startTS endTS N language outputFolder");
			System.exit(1);
		}

	}

}
