/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.rand.image2cluster;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.netbeans.api.progress.ProgressHandle;
import org.rand.kafkaproducer.ImageBlock;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.openide.util.Exceptions;
import org.sleuthkit.autopsy.corecomponentinterfaces.DataSourceProcessorProgressMonitor;

/**
 *
 * @author zev
 */
public class AutopsyKafkaProducer {

    /**
     * @param args the command line arguments
     */
    private static int intNumFiles_ = -1;
    private static int intNumFilesSent_ = 0;
    
    private static ArrayList<Thread> objThreads = new ArrayList<Thread>();
    private static ProgressHandle progressHandle;
        
    public static synchronized void setNumFiles( int intNumFiles ){
        intNumFiles_ = intNumFiles;
        if( intNumFiles_ == intNumFilesSent_ ){
            stopProducer();
        }
    }
    public static synchronized void incrementSentFileCount() {
        
        intNumFilesSent_++;
        
        // Set progress
        if( intNumFiles_ == intNumFilesSent_ ) {
            stopProducer();
            progressHandle.progress(String.format("Sending Image to Cluster... Done"),intNumFiles_);
            progressHandle.finish();
        }
        else if (intNumFiles_ < 0)
        {
            progressHandle.progress(String.format("Sending Image to Cluster...\n" +
                    "Determining number of files...\n" +
                    "Sent: %s", intNumFilesSent_),0);
        }
        else
        {     
            progressHandle.switchToDeterminate(intNumFiles_);
            
            double progress = ((double)intNumFilesSent_ / (double)intNumFiles_ * 100);
            progressHandle.progress(String.format("Sending Image to Cluster... %s%%\nSent: %s/%s",
                    (int)progress, intNumFilesSent_, intNumFiles_), intNumFilesSent_);
        }
    }
    public static void stopProducer(){
        for( Thread objThread : objThreads ){
            objThread.interrupt();
        }
    }
    public static void runProducer(File objDirectory, String strTopic, String strKafkaURL, String strZookeeper ) {
        
        AutopsyKafkaProducer.progressHandle = ProgressHandle.createHandle("Sending image to cluster...");
        KafkaFileConsumer objConsumer = new KafkaFileConsumer(strTopic, strZookeeper);
        Thread objConsumerThread = new Thread( objConsumer );
        KafkaFileProducer objProducer = new KafkaFileProducer(strTopic, strKafkaURL );
        Thread objKafkaProducerThread = new Thread(objProducer);
        DirectoryWatcher objWatcher = new DirectoryWatcher(objProducer, objDirectory);
        Thread objWatchThread = new Thread(objWatcher);
        objThreads.add( objWatchThread);
        objThreads.add( objKafkaProducerThread );
        objThreads.add( objConsumerThread );
        objWatchThread.start();
        AutopsyKafkaProducer.progressHandle.start();
        try {
            Thread.sleep(3000);
        } catch (InterruptedException ex) {
            Exceptions.printStackTrace(ex);
        }
        //objConsumerThread.start();
        objKafkaProducerThread.start();
        /*try {
            objConsumerThread.join();
            objWatchThread.join();
            objKafkaProducerThread.join();
        } catch (InterruptedException objException) {
            objException.printStackTrace();
        }*/
    }

    public static class KafkaFileConsumer implements Runnable {

        private String strTopic_;

        private ConsumerConnector objConsumerConnector_;
        private Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

        private static final DecoderFactory objDecoderFactory_ = DecoderFactory.get();
        private static final SpecificDatumReader<ImageBlock> objImageBlockReader_ = new SpecificDatumReader<ImageBlock>(ImageBlock.SCHEMA$);
        
        public KafkaFileConsumer(String strTopic, String strZookeeper) {
            strTopic_ = strTopic;
            topicCountMap.put(strTopic_, 1);
            Properties props = new Properties();
            props.put("zookeeper.connect", strZookeeper);
            props.put("group.id", "test");
            props.put("zookeeper.session.timeout.ms", "400");
            props.put("zookeeper.sync.time.ms", "200");
            props.put("auto.commit.interval.ms", "1000");
            objConsumerConnector_ = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        }

        public void run() {
            Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = objConsumerConnector_.createMessageStreams(topicCountMap);
            KafkaStream<byte[], byte[]> stream = consumerMap.get(strTopic_).get(0);
            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            while (it.hasNext()) {
                try {
                    BinaryDecoder objDecoder = DecoderFactory.get().binaryDecoder(it.next().message(), null);
                    ImageBlock objBlock = objImageBlockReader_.read(null, objDecoder);
                    //System.out.println(objBlock.toString());
                    if( objBlock.getData().hasArray() ){
                        byte[] objData = objBlock.getData().array();
                        String strHash = DigestUtils.md5Hex( objData );
                        System.out.print( "Hash: " + strHash );
                        if( strHash.equals(objBlock.getLocalHash().toString()) ){
                            System.out.println( " - matches");
                        } else {
                            System.out.println( " - does not match");
                        }
                        
                            
                    }
                } catch (IOException ex) {
                    Logger.getLogger(AutopsyKafkaProducer.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }

    }

    public static class KafkaFileProducer implements Runnable {

        //private LinkedBlockingQueue<File> objFilesToSend = new LinkedBlockingQueue<File>();
        private LinkedList<File> objFilesToSend = new LinkedList<File>();
        private String strTopic_;
        private String strBrokerList_;

        //private Producer objProducer_;
        private KafkaProducer <String, byte[]> objProducer_;
        private static final EncoderFactory objEncoderFactory_ = EncoderFactory.get();
        private static final SpecificDatumWriter<ImageBlock> objImageBlockWriter_ = new SpecificDatumWriter<ImageBlock>(ImageBlock.SCHEMA$);

        public KafkaFileProducer(String strTopic, String strBrokerList) {
            strTopic_ = strTopic;
            strBrokerList_ = strBrokerList;
            Properties objProperties = new Properties();
            //objProperties.setProperty("metadata.broker.list", strBrokerList_);
            objProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,strBrokerList_);
            objProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
            objProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,ByteArraySerializer.class.getName());
            if( System.getenv( "KAFKA_BUFFER_MEMORY_CONFIG" ) != null ){
            	objProperties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, System.getenv( "KAFKA_BUFFER_MEMORY_CONFIG" ));
            }
            else{
            	objProperties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 671088640);
            }
	    if( System.getenv( "KAFKA_SEND_BUFFER_CONFIG" ) != null ){
            	objProperties.put(ProducerConfig.SEND_BUFFER_CONFIG, System.getenv( "KAFKA_SEND_BUFFER_CONFIG" ));
            }
            else{
            	objProperties.put(ProducerConfig.SEND_BUFFER_CONFIG, 13107200);
            }
	    if( System.getenv( "KAFKA_COMPRESSION" ) != null ){
            	objProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, System.getenv( "KAFKA_COMPRESSION" ));
            }
	    if( System.getenv( "KAFKA_PRODUCER_TYPE" ) != null ){
            	objProperties.put("producer.type", System.getenv( "KAFKA_PRODUCER_TYPE" ));
            }
	    if( System.getenv( "KAFKA_BATCH_SIZE" ) != null ){
            	objProperties.put("batch.size", System.getenv( "KAFKA_BATCH_SIZE" ));
            }
	    if( System.getenv( "KAFKA_MAX_REQUEST_SIZE" ) != null ){
            	objProperties.put("max.request.size", System.getenv( "KAFKA_MAX_REQUEST_SIZE" ));
            }
	    if( System.getenv( "KAFKA_LINGER_MS" ) != null ){
            	objProperties.put("linger.ms", System.getenv( "KAFKA_LINGER_MS" ));
            }
	    if( System.getenv( "KAFKA_ACKS" ) != null ){
            	objProperties.put("acks", System.getenv( "KAFKA_ACKS" ));
            }
            //ProducerConfig objConfig = new ProducerConfig(objProperties);
            //objProducer_ = new kafka.javaapi.producer.Producer<Integer, byte[]>(new ProducerConfig(objProperties));
            objProducer_ = new KafkaProducer<String, byte[]>( objProperties );
        }

        public void addFile(File objFile) {
            synchronized( objFilesToSend ){
                System.out.println("Added file: " + objFile.getName());
                objFilesToSend.add(objFile);
                objFilesToSend.notifyAll();
            }
        }

        public void sendFile(File objFile) throws Exception {
            System.out.println("Sending file:" + objFile);
            
            RandomAccessFile objRAF = new RandomAccessFile(objFile, "r");
            FileChannel objFC = objRAF.getChannel();
            MappedByteBuffer objBuffer = objFC.map(FileChannel.MapMode.READ_ONLY, 0, objFC.size());
            
            FileInputStream objFIS = new FileInputStream( objFile );
            String strMD5 = DigestUtils.md5Hex( objFIS );
            objFIS.close();
            
            ImageBlock objBlock = ImageBlock.newBuilder()
                    .setFilename(objFile.getName())
                    .setLocalHash(strMD5)
                    .setData(objBuffer)
                    .build();
            ByteArrayOutputStream objBOS = new ByteArrayOutputStream();
            BinaryEncoder objEncoder = objEncoderFactory_.binaryEncoder(objBOS, null);
            
            objImageBlockWriter_.write(objBlock, objEncoder);
            objEncoder.flush();
            objBOS.close();
            objRAF.close();
            
            byte[] objBytes = objBOS.toByteArray();

            //objProducer_.send(new KeyedMessage<Integer, byte[]>(strTopic_, objBytes));
            ProducerRecord<String,byte[]> record = new ProducerRecord<String,byte[]>( strTopic_, objBytes );
            objProducer_.send( record );
            System.out.println( "  Done Sending " + objFile.getName() );
            incrementSentFileCount();
        }

        public void run() 
        {
            while (true) 
            {

                File[] objBatch = null;
                synchronized( objFilesToSend ){
                    if( objFilesToSend.isEmpty()) {
                        try 
                        {
                            objFilesToSend.wait();
                        } 
                        catch (InterruptedException ex) 
                        {
                            if( intNumFiles_ != intNumFilesSent_ )
                            {
                                Exceptions.printStackTrace(ex);
                            }
                            else{
                                System.out.println( "done sending files");
                                break;
                            }
                        }
                    }

                    objBatch = objFilesToSend.toArray(new File[objFilesToSend.size()]);
                    objFilesToSend.clear();
                }

                System.out.println("Num files:" + intNumFiles_);
                System.out.println("Num files sent:" + intNumFilesSent_);
                System.out.println("Files to send: " + objFilesToSend.size());
                System.out.println("Sending " + objBatch.length + " files");

                for( File objFile : objBatch ){

                    try 
                    {
                        sendFile(objFile);
                    }
                    catch( Exception objException ){
                        objException.printStackTrace();
                        System.exit( -1 );
                    }
                }
                System.out.println( "Done Sending " + objBatch.length + " files");
            //File objFile = this.objFilesToSend.take();
            //sendFile(objFile);
            }


        }
    }

    public static class DirectoryWatcher implements Runnable {

        private File objDirectory_;
        private KafkaFileProducer objProducer_;
        private Path objLastPath_;

        public DirectoryWatcher(KafkaFileProducer objProducer, File objDirectory) {
            objProducer_ = objProducer;
            objDirectory_ = objDirectory;
        }

        public void run() {

            Path objPath = objDirectory_.toPath();

            WatchService objWatchService = null;

            try {
                objWatchService = FileSystems.getDefault().newWatchService();
                objPath.register(objWatchService, StandardWatchEventKinds.ENTRY_CREATE);
            } catch (IOException objException) {
                objException.printStackTrace();
            }

            for (;;) {
                WatchKey objKey = null;
                try {
                    objKey = objWatchService.take();
                } catch (InterruptedException objException) {
                    System.out.println( "Directory watcher interrupted - exiting loop");
                    break;
                }
                for (WatchEvent<?> objEvent : objKey.pollEvents()) {
                    WatchEvent.Kind<?> objKind = objEvent.kind();
                    if (objKind == OVERFLOW) {
                        continue;
                    }
                    WatchEvent<Path> objPathEvent = (WatchEvent<Path>) objEvent;
                    Path objEventPath = objPathEvent.context();

                    Path objResolvedPath = objPath.resolve(objEventPath);

                    System.out.println("File created:" + objResolvedPath);
                    
                    if( objLastPath_ != null ){
                        objProducer_.addFile(objLastPath_.toFile());
                    }
                    if( objResolvedPath.endsWith("done")){
                        System.out.println( "Done received");
                    }
                    objLastPath_ = objResolvedPath;
                }
                boolean blnValid = objKey.reset();
                if (!blnValid) {
                    break;
                }
            }

        }
    }
}
