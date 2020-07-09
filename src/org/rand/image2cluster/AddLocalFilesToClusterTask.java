/*
 * Autopsy Forensic Browser
 *
 * Copyright 2013-2014 Basis Technology Corp.
 * Contact: carrier <at> sleuthkit <dot> org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.rand.image2cluster;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.List;
import java.util.logging.Level;
import java.sql.DriverManager;
import java.sql.Connection;

import java.nio.file.Paths;

import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import kafka.admin.AdminUtils;
import org.openide.util.NbBundle;
import org.sleuthkit.autopsy.casemodule.*;

import org.sleuthkit.autopsy.casemodule.ImageFilePanel;
import org.sleuthkit.autopsy.corecomponentinterfaces.DataSourceProcessorCallback;
import org.sleuthkit.autopsy.corecomponentinterfaces.DataSourceProcessorProgressMonitor;
import org.sleuthkit.autopsy.coreutils.Logger;
import org.sleuthkit.datamodel.AbstractFile;
import org.sleuthkit.datamodel.Content;
import org.sleuthkit.datamodel.CaseDbConnectionInfo;
import org.sleuthkit.autopsy.casemodule.services.FileManager;
import org.sleuthkit.autopsy.core.UserPreferences;
import org.apache.spark.launcher.SparkLauncher;
/**
 * Thread that will add logical files to database, and then kick-off ingest
 * modules. Note: the add logical files task cannot currently be reverted as the
 * add image task can. This is a separate task from AddImgTask because it is
 * much simpler and does not require locks, since the underlying file manager
 * methods acquire the locks for each transaction when adding logical files.
 */
class AddLocalFilesToClusterTask implements Runnable {

    private final Logger logger = Logger.getLogger(AddLocalFilesToClusterTask.class.getName());

    private final String dataSourcePath;
    private final String strTopic_;
    private final DataSourceProcessorProgressMonitor progressMonitor;
    private final DataSourceProcessorCallback callbackObj;

    private final Case currentCase;

    // synchronization object for cancelRequested
    private final Object lock = new Object();
    // true if the process was requested to stop
    private volatile boolean cancelRequested = false;

    private boolean hasCritError = false;

    private final List<String> errorList = new ArrayList<>();
    private final List<Content> newContents = Collections.synchronizedList(new ArrayList<Content>());

    public AddLocalFilesToClusterTask(String dataSourcePath, String strTopic, DataSourceProcessorProgressMonitor aProgressMonitor, DataSourceProcessorCallback cbObj) {
        strTopic_ = strTopic;
        
        currentCase = Case.getCurrentCase();

        this.dataSourcePath = dataSourcePath;
        this.callbackObj = cbObj;
        this.progressMonitor = aProgressMonitor;
    }

	public ProcessBuilder createDC3DDProcess(File objDirectory, File objLogDirectory) throws Exception {
        
        	//String[] paths = dataSourcePath.split(ImageFilePanel.FILES_SEP);
        	String imageFilePath = dataSourcePath;
        
        	int chunkSizeKB = 512;
        	File imageFile = new File(imageFilePath);
        	long imageSize = imageFile.length();
        	int numFiles = (int)Math.ceil((double)(imageSize) / (double)(chunkSizeKB * 1024));
        	AutopsyKafkaProducer.setNumFiles(numFiles);
                    
        	ProcessBuilder objBuilder = new ProcessBuilder();
        	objBuilder.command( 
               		new String[]{
                	"dc3dd",
                	"if=" + imageFilePath, 
                	"hofs=" + objDirectory.getAbsolutePath() + File.separator + "output.0000000000", 
                	"hash=md5", 
                	String.format("ofsz=%sK", chunkSizeKB), 
                	"mlog=" + objLogDirectory.getAbsolutePath() + File.separator + "m_log" 
                } );
            	objBuilder.redirectErrorStream(true);
		return objBuilder;
	}
	public int copyDatabase() throws Exception {
		System.out.println( "copyDatabase - start" );
		CaseMetadata metadata = new CaseMetadata(Paths.get(Case.getCurrentCase().getCaseDirectory() + File.separator + Case.getCurrentCase().getDisplayName() + ".aut"));

		String strCommand = "PGPASSWORD=postgres pg_dump -h desh_postgres_local -U postgres -p " + UserPreferences.getDatabaseConnectionInfo().getPort() +" -C " + metadata.getCaseDatabaseName() + " | bzip2 | sshpass -p postgres ssh -p 30022 -o StrictHostKeyChecking=no postgres@desh_postgres \"bunzip2 | psql -U postgres\" " ;
		System.out.println( strCommand );

        	ProcessBuilder objDBBuilder = new ProcessBuilder();
		objDBBuilder.command( 
               		new String[]{
                	"/bin/bash",
			"-c",
			strCommand
		} );

            	objDBBuilder.redirectErrorStream(true);

            	Process objDBProcess = objDBBuilder.start();

		BufferedReader dbreader = new BufferedReader(new InputStreamReader(objDBProcess.getInputStream()));
		String dbline;
            	while ((dbline = dbreader.readLine()) != null){
               		System.out.println("db copy: " + dbline);
            	}
            	int intDbExit = objDBProcess.waitFor();
            	System.out.println( "Exit code:" + intDbExit);
		System.out.println( "copyDatabase - finish" );
		return intDbExit;
	}
	public void setupKafkaStream( String kafkaURL, String zookeeperURL, String strTopicPartitionCount) throws Exception {
		System.out.println( "setupKafkaStream - started" );

	    	ZkClient zkClient = new ZkClient( zookeeperURL, 10000, 10000, ZKStringSerializer$.MODULE$);
	 	AdminUtils.createTopic(zkClient, strTopic_, Integer.parseInt( strTopicPartitionCount ), 1, new Properties());
		System.out.println( "setupKafkaStream - finished" );
	}
	public void checkDatabaseReady() throws Exception {
		System.out.println( "checkDatabaseReady - started" );
                Class.forName("org.postgresql.Driver");
		boolean blnConnected = false;
		CaseDbConnectionInfo db = UserPreferences.getDatabaseConnectionInfo();
		CaseMetadata metadata = new CaseMetadata(Paths.get(Case.getCurrentCase().getCaseDirectory() + File.separator + Case.getCurrentCase().getDisplayName() + ".aut"));
		while( blnConnected == false ){
			try{
				System.out.println( "Trying to connect to database..." );
				Connection dbConnection = DriverManager.getConnection("jdbc:postgresql://desh_postgres:32432/" + metadata.getCaseDatabaseName(), db.getUserName(), db.getPassword()); //NON-NLS
				dbConnection.close();
				blnConnected = true;
			} catch (Exception objException ){
				System.out.println( "Database not ready yet..." );
				Thread.sleep( 10000 );
			}
		}
		System.out.println( "checkDatabaseReady - finished" );
	}
	public int startSparkJob( String strTopicPartitionCount ) throws Exception{
		System.out.println( "startSparkJob - started" );
		if( System.getenv( "SPARK_KAFKA_PORT" ) != null ){
			strKafkaPort = System.getenv( "SPARK_KAFKA_PORT" );
		}
		if( System.getenv( "SPARK_POSTGRES_HOST" ) != null ){
			strPostgresHost = System.getenv( "SPARK_POSTGRES_HOST" );
		}
		if( System.getenv( "SPARK_POSTGRES_PORT" ) != null ){
			strPostgresPort = System.getenv( "SPARK_POSTGRES_PORT" );
		}
		if( System.getenv( "SPARK_MASTER_URL" ) != null ){
			strSparkURL = System.getenv( "SPARK_MASTER_URL" );
		}
                
                String strSparkBatchInterval = "60";
                if( System.getenv( "SPARK_BATCH_INTERVAL" ) != null ){
			strSparkBatchInterval = System.getenv( "SPARK_BATCH_INTERVAL" );
		}
                String strSparkKafkaServer = "kafka-service";
                if( System.getenv( "SPARK_KAFKA_SERVER" ) != null ){
			strSparkKafkaServer = System.getenv( "SPARK_KAFKA_SERVER" );
		}
	    	Process spark = new SparkLauncher()
                  .setAppResource("/mnt/assembly/desh-streaming-job-assembly-1.0.jar")
                  .addAppArgs( strTopic_, "local_mem_fs", "/mnt/storage/", "/mnt/casefiles/", strPostgresHost, "solr-service", strKafkaPort, strSparkKafkaServer, strTopicPartitionCount,  strSparkBatchInterval, strPostgresPort )
                  .addJar("/mnt/assembly/postgresql-9.4-1203.jdbc4.jar")
                  .setVerbose(true)
                  .setDeployMode("cluster")
        	  .setMainClass("NotSimpleApp")
         	  .setMaster("spark://" + strSparkURL )
         	  .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
                  //.setConf(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS, "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=7777") //UNCOMMENT TO DEBUG EXECUTOR
         	  .setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, "/mnt/assembly/postgresql-9.4-1203.jdbc4.jar")
       		  .launch();

	    	new Thread(){ public void run() {
	    		try{
            			BufferedReader sparkReader = new BufferedReader(new InputStreamReader(spark.getInputStream()));
            			String line;
            			while ((line = sparkReader.readLine()) != null){
               		 		System.out.println("spark: " + line);
            			}
	    		} catch ( Exception objException ){
				objException.printStackTrace();
	    		}
	    	}}.start();

	    	new Thread(){ public void run() {
			try{
            			BufferedReader sparkReader = new BufferedReader(new InputStreamReader(spark.getErrorStream()));
            			String line;
            			while ((line = sparkReader.readLine()) != null){
               				System.out.println("spark: " + line);
            			}
	    		} catch ( Exception objException ){
				objException.printStackTrace();
	    		}
	    	}}.start();
       	    	int intSparkExit = spark.waitFor();
            	System.out.println( "Spark exit code:" + intSparkExit);
		System.out.println( "startSparkJob - finshed" );
		return intSparkExit;
	}
	public int startKafkaStream(File objDirectory, File objLogDirectory, String kafkaURL, String zookeeperURL ) throws Exception {
		System.out.println( "startKafkaStream - started" );
		ProcessBuilder objBuilder = createDC3DDProcess( objDirectory, objLogDirectory);
		AutopsyKafkaProducer.runProducer(objDirectory, strTopic_, kafkaURL, zookeeperURL );
            	Process objProcess = objBuilder.start();
            	BufferedReader reader = new BufferedReader(new InputStreamReader(objProcess.getInputStream()));
            	String line;
            	while ((line = reader.readLine()) != null){
               		System.out.println("dc3dd: " + line);
            	}
           	int intExit = objProcess.waitFor();
            	System.out.println( "Exit code:" + intExit);
            
            	FileWriter objDoneFile = new FileWriter(objDirectory.getAbsolutePath() + File.separator + "done");
            	objDoneFile.write("done");
            	objDoneFile.close();
		System.out.println( "startKafkaStream - finished" );
		return intExit;
	}
    /**
     * Add local files and directories to the case
     *
     * @return
     *
     * @throws Exception
     */
    @Override
    public void run() {

	try{
		Thread.sleep( 3000 );
		String strDC3DDDir = Case.getCurrentCase().getCaseDirectory() ;
		if( System.getenv( "DC3DD_DIR" ) != null ){
			strDC3DDDir = System.getenv( "DC3DD_DIR" ) + File.separator + Case.getCurrentCase().getName();
		}
		System.out.println( "DC3DD Dir: " + strDC3DDDir );

        	File objDirectory = new File( strDC3DDDir + File.separator + "dc3dd_output" );
        	if( objDirectory.exists() == false ){
            		objDirectory.mkdirs();
        	}
        	File objLogDirectory = new File( strDC3DDDir + File.separator + "dc3dd_log" );
        	if( objLogDirectory.exists() == false ){
            		objLogDirectory.mkdirs();
        	}

		final String strTopicPartitionCount = System.getenv( "TOPIC_PARTITION_COUNT" ) == null ? "1" : System.getenv( "TOPIC_PARTITION_COUNT" );

		if( System.getenv( "KAFKA_URL" ) != null ){
			kafkaURL = System.getenv( "KAFKA_URL" );	
		}
		if( System.getenv( "ZOOKEEPER_URL" ) != null ){
			zookeeperURL = System.getenv( "ZOOKEEPER_URL" );	
		}
		System.out.println( "Kafka: " + kafkaURL );
		System.out.println( "Zookeeper: " + zookeeperURL );
		System.out.println( "Partition Count: " + strTopicPartitionCount );

		if( System.getenv( "SKIP_DATABASE_COPY" ) == null ){
			int intExit = copyDatabase();
			if( intExit != 0 ){
				System.out.println( "error copying database" );
				System.exit( -1 );
			}
		}
		else{
			System.out.println( "skipping copying database" );
		}

		setupKafkaStream( kafkaURL, zookeeperURL, strTopicPartitionCount );

		Thread objDatabaseThread = new Thread(){ 
			public void run(){
				try{
					if( System.getenv( "SKIP_DATABASE_COPY" ) == null ){
						checkDatabaseReady();
					}
					else{
						System.out.println( "skipping checking database" );
					}
					int intSparkExit = startSparkJob( strTopicPartitionCount );
					if( intSparkExit != 0 ){
						System.out.println( "error starting spark" );
						System.exit( -1 );
					}
				} catch (Exception objException ){
					objException.printStackTrace();
				}
			} 
		};

		objDatabaseThread.start();

		int intExit = startKafkaStream(objDirectory, objLogDirectory, kafkaURL, zookeeperURL );
		if( intExit != 0 ){
			System.out.println( "error starting kafka stream" );
			System.exit( -1 );
		}

		objDatabaseThread.join();

        } catch (Exception objException) {
            objException.printStackTrace();
        }
        // handle  done
        postProcess();

    }

    private void postProcess() {

        if (cancelRequested() || hasCritError) {
            logger.log(Level.WARNING, "Handling errors or interruption that occured in logical files process");  //NON-NLS
        }
        if (!errorList.isEmpty()) {
            //data error (non-critical)
            logger.log(Level.WARNING, "Handling non-critical errors that occured in logical files process"); //NON-NLS
        }

        if (!(cancelRequested() || hasCritError)) {
            progressMonitor.setProgress(100);
            progressMonitor.setIndeterminate(false);
        }

        // invoke the callBack, unless the caller cancelled 
        if (!cancelRequested()) {
            doCallBack();
        }

    }

    /*
     * Call the callback with results, new content, and errors, if any
     */
    private void doCallBack() {
        DataSourceProcessorCallback.DataSourceProcessorResult result;

        if (hasCritError) {
            result = DataSourceProcessorCallback.DataSourceProcessorResult.CRITICAL_ERRORS;
        } else if (!errorList.isEmpty()) {
            result = DataSourceProcessorCallback.DataSourceProcessorResult.NONCRITICAL_ERRORS;
        } else {
            result = DataSourceProcessorCallback.DataSourceProcessorResult.NO_ERRORS;
        }

        // invoke the callback, passing it the result, list of new contents, and list of errors
        if (callbackObj != null)
        {
            // TODO(ttran): testing
            System.out.println("** Callback done **");
            // callbackObj.done(result, errorList, newContents);
        }
    }

    /*
     * cancel the files addition, if possible
     */
    public void cancelTask() {
        synchronized (lock) {
            cancelRequested = true;
        }
    }

    private boolean cancelRequested() {
        synchronized (lock) {
            return cancelRequested;
        }
    }

    /**
     * Updates the wizard status with logical file/folder
     */
    private class LocalFilesAddProgressUpdater implements FileManager.FileAddProgressUpdater {

        private int count = 0;
        private final DataSourceProcessorProgressMonitor progressMonitor;

        LocalFilesAddProgressUpdater(DataSourceProcessorProgressMonitor progressMonitor) {

            this.progressMonitor = progressMonitor;
        }

        @Override
        public void fileAdded(final AbstractFile newFile) {
            if (count++ % 10 == 0) {
                progressMonitor.setProgressText(
                        NbBundle.getMessage(this.getClass(), "AddLocalFilesTask.localFileAdd.progress.text",
                                newFile.getParentPath(), newFile.getName()));
            }
        }
    }
}
