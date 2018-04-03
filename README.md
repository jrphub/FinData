Usage :
-----
1. Download and extract the code in your workspace
2. In eclipse, import the project as Maven project (ref: https://javapapers.com/java/import-maven-project-into-eclipse/)
3. delete metastore_db folder
4. In AppMainTwo.java file, change input file path in the code
    //Modify these 3 values
	String sparkWarehouseDir = "file:///home/jrp/workspace_1/FinData/spark-warehouse";
	String inputPath = "file:///home/jrp/data/Data/20180319-sms-request-33.txt";
	String templateFilePath = "/home/jrp/workspace_1/FinData/input-data/templateFile.txt";

5. Right click on project and go to run as -> maven build 
   Then in goals, write "mvn clean install"
   Then click on "Run"
6. After build gets finished, Right click on AppMainTwo.java and run as Java Application
7. Check console for any error or output
