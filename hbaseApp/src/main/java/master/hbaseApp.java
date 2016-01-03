package master;

import java.io.BufferedReader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Hashtable;

import javax.crypto.KeyGenerator;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
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
	 * Method to perform the first query
	 * Given a language (lang), do find the Top-N most used words for the given language in
	 * a time interval defined with a start and end timestamp. Start and end timestamp are
	 * in milliseconds.      
	 */
	private void firstQuery() {
	}

	/**
	 * Method to perform the second query
	 * Do find the list of Top-N most used words for each language in a time interval defined
     * with the provided start and end timestamp. Start and end timestamp are in
     * milliseconds. 
	 */
	private void secondQuery() {
	}

	/**
	 * Method to perform the third query
	 * Do find the Top-N most used words and the frequency of each word regardless the
     * language in a time interval defined with the provided start and end timestamp. Start
     * and end timestamp are in milliseconds.      
	 */
	private void thirdQuery() {
	}

	/**
	 * Method to create the table in hbase
	 */
	private void createTable() {
		// Instantiating configuration class
		Configuration conf = HBaseConfiguration.create();

		// Instantiating HbaseAdmin class
		try {
			admin = new HBaseAdmin(conf);
  
			// Instantiating table descriptor class
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("TopTopics"));

			// Adding column families to table descriptor
			tableDescriptor.addFamily(new HColumnDescriptor("hashtags"));

			if(!admin.tableExists("TopTopics"))// Execute the table through admin			
			 admin.createTable(tableDescriptor);
			 table = new HTable(conf, "TopTopics");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(" Table created ");
	}

	/**
	 * Method to insert into the hbase table
	 */
	private void insertIntoTable(String timestamp, String lang,String hashtag, String counts) {
	    byte[] key = KeyGenerator.generateKey(timestamp,lang);
	    Get get = new Get(key);
	    Result res = table.get(get);
	    if(res == null && res.isEmpty()){ // insert in table
	    	Put put = new Put(key);
	    	put.add(Bytes.toBytes("hashtags"),Bytes.toBytes("topic"),Bytes.toBytes(hashtag));
	    	put.add(Bytes.toBytes("hashtags"),Bytes.toBytes("counts"),Bytes.toBytes(counts));
	    	table.put(put);
	    }
	}
	
	/**
	 * Method to load the files in hbase
	 */
	private void load() {
	   System.out.println("Loading data into hbase");	
		Hashtable<String, Integer> hashtags = new Hashtable<String, Integer>();
		try(BufferedReader br = new BufferedReader(new FileReader(dataFolder))) {
			for(String line; (line = br.readLine()) != null; ) {
				// process the line.
				String[] fields = line.split(",");
				int pos = 0;
				String timestamp = fields[pos++];
				String lang = fields[pos++];
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
	 * Method to set the necessary parameters
	 * @param args Arguments passed by command line       
	 */
	private void setContext(String[] args, int mode) {
		switch (mode) {
		case 1: 	dataFolder=args[1];
		break;
		case 2: 	startTS=Integer.parseInt(args[1]);
		endTS=Integer.parseInt(args[2]);
		N=Integer.parseInt(args[3]);
		languages=args[4].split(",");;
		outputFolderPath=args[5];
		break;
		case 3: 	startTS=Integer.parseInt(args[1]);
		endTS=Integer.parseInt(args[2]);
		N=Integer.parseInt(args[3]);
		languages=args[4].split(",");;
		outputFolderPath=args[5];
		break;
		case 4: 	startTS=Integer.parseInt(args[1]);
		endTS=Integer.parseInt(args[2]);
		N=Integer.parseInt(args[3]);
		outputFolderPath=args[4];
		break;     	
		}
	}

	/**
	 * Method to start the hbase app with the selected query
	 * @param mode Mode to start the app. Mode 1 reads from file. Mode 2 reads from twitter API.     
	 */
	private void start(int mode) {

		switch (mode) {
		case 1: 	load();
		break;
		case 2: 	firstQuery();
		break;
		case 3: 	secondQuery();
		break;
		case 4: 	thirdQuery();
		break;     	
		}
	}

	/**
	 * Main method
	 * @param args Arguments: mode dataFolder startTS endTS N language outputFolder    
	 * @throws java.lang.Exception    
	 */
	public static void main(String[] args) throws Exception {
		int mode = 0;
		if (args.length > 0) {
			System.out.println("Started hbaseApp with mode: " + args[0]);
			try {
				mode = Integer.parseInt(args[0]);
				if (mode==1 && args.length!=2 ) {
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
				if (mode==4 && args.length!=5 ) {
					System.out.println("To start the App with mode 4 it is required the mode startTS endTS N outputFolder");
					System.exit(1);  
				}  

				hbaseApp app = new hbaseApp();
				app.setContext(args,mode);
				app.start(mode);
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
