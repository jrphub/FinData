package com.analysis.findata.FinData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;


/*
 * Load data from file to hive
 * 
 * Create an empty table on hive before running this application, if you are using external hive, else not needed
 * CREATE TABLE IF NOT EXISTS default.FinData (account_no STRING, amount LONG, service_provider STRING, date DATE, receiver STRING)
 * 
 * args[0]=file:///home/jrp/workspace_1/FinData/spark-warehouse
 * args[1]=file:///home/jrp/data/Data/20180319-sms-request-33.txt
 * args[2]=/home/jrp/workspace_1/FinData/input-data/templateFile.txt
 * 
 */
public class AppMainTwo {
	
	private	static List<String> templates = new ArrayList<String>();

	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		
		//if running the application using jar
		/*if (args.length != 2) {
			System.out.println("Invalid arguments");
			System.exit(1);
		}*/
		
		//String sparkWarehouseDir = args[0];
		//String inputPath = args[1];
		//String templateFilePath = args[2];
		
		//Modify these 3 values
		String sparkWarehouseDir = "file:///home/jrp/workspace_1/FinData/spark-warehouse";
		String inputPath = "file:///home/jrp/data/Data/20180319-sms-request-33.txt";
		String templateFilePath = "/home/jrp/workspace_1/FinData/input-data/templateFile.txt";
		
		SparkConf conf = new SparkConf()
				.setMaster("local[*]")
				.setAppName("Finance Data Analysis part2")
				//Not using hive set up via hive-site.xml, core-site.xml
				.set("spark.sql.warehouse.dir",
						sparkWarehouseDir);
		
		//System.setProperty("HADOOP_USER_NAME", "huser");
		//This will create metastore_db in project directory
		SparkSession spark = SparkSession.builder().config(conf)
				.enableHiveSupport().getOrCreate();
		
		//create empty table to append data later
		//spark.sql("");
		loadTemplates(templateFilePath);
		//load the file from local (file://) or hdfs ("hdfs://)
		JavaRDD<String> finRDD = spark.sparkContext().textFile(inputPath,3).toJavaRDD();
		
		//Filter data matching with template only
		JavaRDD<String> postStr = finRDD.filter(new Function<String, Boolean>() {
			
			public Boolean call(String str) throws Exception {
				String[] elements = str.split(",");
				String msg = null;
				if (elements.length > 22) {
					msg = str.split(",")[22];
				}
				
				if (msg != null && matchTemplate(msg)) {
					return true;
				}
				//debug
				/*if (msg.contains("Your buy order dated")) {
					System.out.println(msg);
				}*/
				//System.out.println(msg);
				return false;
			}
		});
		
		//Parse message and create collection of message POJO
		JavaRDD<FinDataTwo> finDataRDD = postStr.map(new Function<String, FinDataTwo>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public FinDataTwo call(String str) throws Exception {
				FinDataTwo finObj = new FinDataTwo();
				//Rows matching Template1 and Template2
				String[] strAttributes = str.split(",");
				finObj.setDate(strAttributes[0]);
				finObj.setSmsReq(strAttributes[2]);
				finObj.setBankName(strAttributes[7]);
				finObj.setMobileNo(strAttributes[9]);
				finObj.setDndStatus(strAttributes[16]);
				//Now parsing msg for meaningful data
				String msg = strAttributes[22];
				if (matchTemp1(msg)) {
					//only rows matching with template1
					String[] msgWords = msg.split(" ");
					finObj.setActNumber(msgWords[11]);
					finObj.setAmt(msgWords[14]);
					finObj.setMsgDate(msgWords[19]);
					finObj.setTemplate(01);
				}
				
				if (matchTemp2(strAttributes[22])) {
					//only rows matching with template2
					String[] msgWords = msg.split(" ");
					finObj.setMsgDate(msgWords[4]);
					finObj.setShareName(msgWords[6]);
					finObj.setNumberShare(msgWords[msgWords.length-11]);
					finObj.setAvgShare(StringUtils.chop(msgWords[msgWords.length-1]));
					finObj.setTemplate(02);
				}
				return finObj;
			}
			
		});
		
		//Create dataframe, each row corresponds to data POJO
		Dataset<Row> df = spark.createDataFrame(finDataRDD,
                FinDataTwo.class);
		//spark.sql("drop table if exists default.spark.sql("select count(*) from default.FinDataTwo where avgShare is not null").show();FinData");
		//Save to hive table
		//for external hive table, the table must be created before to be appended or overwritten by sql provided
		df.write().mode(SaveMode.Overwrite).saveAsTable("default.FinDataTwo");
		//df.write().mode(SaveMode.Overwrite).insertInto("default.FinData");
		
		//check row count of the table
		spark.sql("select count(*) as template1 from default.FinDataTwo where template=01").show();
		spark.sql("select count(*) as template2 from default.FinDataTwo where template=02").show();
		spark.sql("select count(*) as temp1_temp2 from default.FinDataTwo").show();
		
		long endTime=System.currentTimeMillis();
		long totalTime=endTime-startTime;
		System.out.println("Total Time Taken : " + totalTime/1000 + " seconds");
		
	}

	private static void loadTemplates(String templateFile) {
		try (BufferedReader bfr = new BufferedReader(new FileReader (new File(templateFile)))) {
			String line=null;
			while((line=bfr.readLine()) != null) {
				templates.add(line);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected static boolean matchTemplate(String msg) {
		if (matchTemp1(msg)) {
			return true;
		}
		
		if (matchTemp2(msg)) {
			return true;
		}
		return false;
	}

	private static boolean matchTemp1(String msg) {
		String template = templates.get(0);
		String pattnFrmTemplate = template.replaceAll("\\.", "\\\\."); // escape "." 
	    pattnFrmTemplate = pattnFrmTemplate.replace("{garbage}", "(.*)"); // capturing group 1
	    pattnFrmTemplate = pattnFrmTemplate.replace("{actNumber}", "(.*)"); // capturing group 2
	    pattnFrmTemplate = pattnFrmTemplate.replace("{amt}", "(.*)");
	    pattnFrmTemplate = pattnFrmTemplate.replace("{msgDate}", "(.*)");
	    pattnFrmTemplate = pattnFrmTemplate.replace("{ignore}", "(.*)");
	    
	    Pattern p = Pattern.compile(pattnFrmTemplate);
	    Matcher m = p.matcher(msg);
	    if (m.matches()) {
	    	return true;
	    }
		
		return false;
	}
	
	private static boolean matchTemp2(String msg) {
		String template = templates.get(1);
		String pattnFrmTemplate = template.replaceAll("\\.", "\\\\."); // escape "." 
	    pattnFrmTemplate = pattnFrmTemplate.replace("{msgData}", "(.*)"); // capturing group 1
	    pattnFrmTemplate = pattnFrmTemplate.replace("{shareName}", "(.*)"); // capturing group 2
	    pattnFrmTemplate = pattnFrmTemplate.replace("{numberShare}", "(.*)");
	    pattnFrmTemplate = pattnFrmTemplate.replace("{avgShare}", "(.*)");
	    pattnFrmTemplate = pattnFrmTemplate.replace("{extraSpace}", "(\\s*?)");
	    
	    Pattern p = Pattern.compile(pattnFrmTemplate);
	    Matcher m = p.matcher(msg);
	    if (m.matches()) {
	    	return true;
	    }
		return false;
	}

}
