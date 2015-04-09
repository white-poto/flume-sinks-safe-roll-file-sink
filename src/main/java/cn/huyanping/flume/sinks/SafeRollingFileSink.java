package cn.huyanping.flume.sinks;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.EventSerializerFactory;

public class SafeRollingFileSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory
            .getLogger(SafeRollingFileSink.class);
    private static final long defaultRollInterval = 30;
    private static final int defaultBatchSize = 100;

    private int batchSize = defaultBatchSize;

    private File directory;
    private long rollInterval;
    //已完成写入的文件，是否使用后缀
    private boolean useFileSuffix;
    //后缀名
    private String fileSuffix;
    //是否移动文件
    private boolean moveFile;
    //移动文件目录
    private File targetDirector;
    private OutputStream outputStream;
    private ScheduledExecutorService rollService;

    private String serializerType;
    private Context serializerContext;
    private EventSerializer serializer;

    private SinkCounter sinkCounter;

    private SafePathManager pathController;
    private volatile boolean shouldRotate;

    public SafeRollingFileSink() {
        //原pathController轮转文件名可能会出现重复情况，修正
        pathController = new SafePathManager();
        shouldRotate = false;
    }

    public void configure(Context context) {

        String directory = context.getString("sink.directory");
        String rollInterval = context.getString("sink.rollInterval");
        useFileSuffix = context.getBoolean("sink.useFileSuffix", false);
        fileSuffix = context.getString("sink.fileSuffix", "");
        moveFile = context.getBoolean("sink.moveFile", false);
        String targetDirectory = context.getString("sink.targetDirectory", directory);

        serializerType = context.getString("sink.serializer", "TEXT");
        serializerContext =
                new Context(context.getSubProperties("sink." +
                        EventSerializer.CTX_PREFIX));

        Preconditions.checkArgument(directory != null, "Directory may not be null");
        Preconditions.checkNotNull(serializerType, "Serializer type is undefined");

        if (rollInterval == null) {
            this.rollInterval = defaultRollInterval;
        } else {
            this.rollInterval = Long.parseLong(rollInterval);
        }

        batchSize = context.getInteger("sink.batchSize", defaultBatchSize);

        this.directory = new File(directory);
        this.targetDirector = new File(targetDirectory);

        //检查目录权限
        if(!this.directory.exists()){
            if(!this.directory.mkdirs()){
                throw new IllegalArgumentException("sink.directory is not a directory");
            }
        }else if(!this.directory.canWrite()){
            throw new IllegalArgumentException("sink.directory can not write");
        }

        //检查目标目录权限
        if(!this.targetDirector.exists()){
            if(!this.targetDirector.mkdirs()){
                throw new IllegalArgumentException("sink.directory is not a directory");
            }
        }else if(!this.targetDirector.canWrite()){
            throw new IllegalArgumentException("sink.directory can not write");
        }

        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(getName());
        }
    }

    @Override
    public void start() {
        logger.info("Starting {}...", this);
        sinkCounter.start();
        super.start();

        pathController.setBaseDirectory(directory);
        if(rollInterval > 0){

            rollService = Executors.newScheduledThreadPool(
                    1,
                    new ThreadFactoryBuilder().setNameFormat(
                            "rollingFileSink-roller-" +
                                    Thread.currentThread().getId() + "-%d").build());

      /*
       * Every N seconds, mark that it's time to rotate. We purposefully do NOT
       * touch anything other than the indicator flag to avoid error handling
       * issues (e.g. IO exceptions occuring in two different threads.
       * Resist the urge to actually perform rotation in a separate thread!
       */
            rollService.scheduleAtFixedRate(new Runnable() {

                public void run() {
                    logger.debug("Marking time to rotate file {}",
                            pathController.getCurrentFile());
                    shouldRotate = true;
                }

            }, rollInterval, rollInterval, TimeUnit.SECONDS);
        } else{
            logger.info("RollInterval is not valid, file rolling will not happen.");
        }
        logger.info("RollingFileSink {} started.", getName());
    }

    public Status process() throws EventDeliveryException {
        if (shouldRotate) {
            logger.debug("Time to rotate {}", pathController.getCurrentFile());

            if (outputStream != null) {
                logger.debug("Closing file {}", pathController.getCurrentFile());

                try {
                    serializer.flush();
                    serializer.beforeClose();
                    outputStream.close();
                    sinkCounter.incrementConnectionClosedCount();
                    shouldRotate = false;
                    //文件名加后缀、移动文件
                    if(!rename(pathController.getCurrentFile())){
                        logger.error("Rename completed file failed");
                        throw new IOException("Ranme completed file failed");
                    }
                } catch (IOException e) {
                    sinkCounter.incrementConnectionFailedCount();
                    throw new EventDeliveryException("Unable to rotate file "
                            + pathController.getCurrentFile() + " while delivering event", e);
                } finally {
                    serializer = null;
                    outputStream = null;
                }

                pathController.rotate();
            }
        }

        if (outputStream == null) {
            File currentFile = pathController.getCurrentFile();
            logger.debug("Opening output stream for file {}", currentFile);
            try {
                outputStream = new BufferedOutputStream(
                        new FileOutputStream(currentFile));
                serializer = EventSerializerFactory.getInstance(
                        serializerType, serializerContext, outputStream);
                serializer.afterCreate();
                sinkCounter.incrementConnectionCreatedCount();
            } catch (IOException e) {
                sinkCounter.incrementConnectionFailedCount();
                throw new EventDeliveryException("Failed to open file "
                        + pathController.getCurrentFile() + " while delivering event", e);
            }
        }

        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Event event = null;
        Status result = Status.READY;

        try {
            transaction.begin();
            int eventAttemptCounter = 0;
            for (int i = 0; i < batchSize; i++) {
                event = channel.take();
                if (event != null) {
                    sinkCounter.incrementEventDrainAttemptCount();
                    eventAttemptCounter++;
                    serializer.write(event);

          /*
           * FIXME: Feature: Rotate on size and time by checking bytes written and
           * setting shouldRotate = true if we're past a threshold.
           */

          /*
           * FIXME: Feature: Control flush interval based on time or number of
           * events. For now, we're super-conservative and flush on each write.
           */
                } else {
                    // No events found, request back-off semantics from runner
                    result = Status.BACKOFF;
                    break;
                }
            }
            serializer.flush();
            outputStream.flush();
            transaction.commit();
            sinkCounter.addToEventDrainSuccessCount(eventAttemptCounter);
        } catch (Exception ex) {
            transaction.rollback();
            throw new EventDeliveryException("Failed to process transaction", ex);
        } finally {
            transaction.close();
        }

        return result;
    }

    @Override
    public void stop() {
        logger.info("RollingFile sink {} stopping...", getName());
        sinkCounter.stop();
        super.stop();

        if (outputStream != null) {
            logger.debug("Closing file {}", pathController.getCurrentFile());

            try {
                serializer.flush();
                serializer.beforeClose();
                outputStream.close();
                sinkCounter.incrementConnectionClosedCount();
                //文件名加后缀、移动文件
                if(!rename(pathController.getCurrentFile())){
                    logger.error("Rename completed file failed");
                    throw new IOException("Ranme completed file failed");
                }
            } catch (IOException e) {
                sinkCounter.incrementConnectionFailedCount();
                logger.error("Unable to close output stream. Exception follows.", e);
            } finally {
                outputStream = null;
                serializer = null;
            }
        }
        if(rollInterval > 0){
            rollService.shutdown();

            while (!rollService.isTerminated()) {
                try {
                    rollService.awaitTermination(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    logger
                            .debug(
                                    "Interrupted while waiting for roll service to stop. " +
                                            "Please report this.", e);
                }
            }
        }
        logger.info("RollingFile sink {} stopped. Event metrics: {}",
                getName(), sinkCounter);
    }

    public File getDirectory() {
        return directory;
    }

    public void setDirectory(File directory) {
        this.directory = directory;
    }

    public long getRollInterval() {
        return rollInterval;
    }

    public void setRollInterval(long rollInterval) {
        this.rollInterval = rollInterval;
    }

    private boolean rename(File current) {
        if (current.length() == 0L) {
            logger.info("Delete empty file{}", current.getName());
            return current.delete();
        }
        if(useFileSuffix && moveFile){
            return current.renameTo(new File(this.targetDirector, current.getName() + fileSuffix));
        }else if(useFileSuffix){
            return current.renameTo(new File(this.directory, current.getName() + fileSuffix));
        }else if(moveFile){
            return current.renameTo(new File(this.targetDirector, current.getName()));
        }else{
            return true;
        }
    }
}



