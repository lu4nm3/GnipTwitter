package com.attensity.gnip.twitter;

import com.attensity.mongo.MongoConnector;
import com.attensity.mongo.MongoWriter;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Encoder;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author lmedina
 */
public class StreamingConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingConnection.class);
    public static final String DEFAULT_GNIP_STREAM_TYPE = "POWERTRACK";
    public static final int DEFAULT_RUN_DURATION_MS = 20000;
    public static final int DEFAULT_MONGO_SAVE_COUNT = 100;
    public static final String RUN_DURATION_MILLISECONDS = "runDurationMilliseconds";
    public static final String MONGO_SAVE_COUNT = "mongoSaveCount";
    public static final String MONGO_WRITER_COUNT = "mongoWriterCount";
    public static final String GNIP_URL = "gnipUrl";
    public static final String MONGO_COLLECTION_NAME = "mongoCollectionName";
    public static final String DECAHOSE = "DECAHOSE";
    public static final String POWERTRACK = "POWERTRACK";

    private static StreamingConnection streamingConnection = new StreamingConnection();

    Properties properties;
    private String username;
    private String password;
    private String streamURL;
    private String charset;

    private HttpURLConnection connection;
    private InputStream inputStream;

    private ExecutorService executorService;

    private ExecutorService mongoExecutorService;
    private BlockingQueue<String> messageQueue;
    private MongoConnector mongoConnector;

    private ScheduledExecutorService rateTrackingExecutorService;
    private long TIME_SECONDS;
    private int saveCount;
    private AtomicLong oldMessageCount = new AtomicLong(0);
    private AtomicLong currentMessageCount = new AtomicLong(0);

    public static void main(String... args) throws IOException {
        try {
            String streamType = DEFAULT_GNIP_STREAM_TYPE;
            long runDurationMilliseconds = DEFAULT_RUN_DURATION_MS;
            int mongoSaveCount = DEFAULT_MONGO_SAVE_COUNT;

            if(args.length < 3) {
                printUsage();
                System.exit(0);
            }

            if (args.length == 3) {
                streamType = args[0];
                mongoSaveCount = Integer.parseInt(args[1]);
                runDurationMilliseconds = Long.parseLong(args[2]);
            }

            if(StringUtils.isEmpty(streamType)) {
                streamType = DEFAULT_GNIP_STREAM_TYPE;
            }

            if(mongoSaveCount <= 0) {
                mongoSaveCount = DEFAULT_MONGO_SAVE_COUNT;
            }

            if(runDurationMilliseconds <= 0) {
                runDurationMilliseconds = DEFAULT_RUN_DURATION_MS;
            }

            LOGGER.info("Running stream of type: " + streamType);
            streamingConnection.init(streamType, mongoSaveCount, runDurationMilliseconds);
            streamingConnection.start();

            long runDurationMs = Long.parseLong(streamingConnection.getProperties().getProperty("runDurationMilliseconds"));
            Thread.sleep(runDurationMs);

            streamingConnection.stop();
        } catch (Exception e) {
            LOGGER.error("Failed to start daemon.", e);
        }
    }

    public void configure(String streamType, int mongoSaveCount, long runDurationMilliseconds) {
        properties = new Properties();
        properties.setProperty(RUN_DURATION_MILLISECONDS, ""+runDurationMilliseconds);
        properties.setProperty(MONGO_SAVE_COUNT, ""+mongoSaveCount);
        properties.setProperty(MONGO_WRITER_COUNT, "5");
        switch (streamType.toUpperCase()) {
            case DECAHOSE:    properties.setProperty(GNIP_URL, "https://stream.gnip.com:443/accounts/Attensity/publishers/twitter/streams/decahose/prod.json");
                                properties.setProperty(MONGO_COLLECTION_NAME, "decahose");
                                break;
            case POWERTRACK:  properties.setProperty(GNIP_URL, "https://stream.gnip.com:443/accounts/Attensity/publishers/twitter/streams/track/prod.json");
                                properties.setProperty(MONGO_COLLECTION_NAME, "powertrack");
                                break;
            default:
        }
    }

    public void init(String streamType, int mongoWriteCount, long runDurationMilliseconds) throws Exception {
        configure(streamType, mongoWriteCount, runDurationMilliseconds);

        username = "ebradley@attensity.com";
        password = "@tt3ns1ty";
        streamURL = properties.getProperty(GNIP_URL);
        charset = "UTF-8";

        messageQueue = new LinkedBlockingQueue<String>();
        mongoConnector = new MongoConnector();
        saveCount = mongoWriteCount;

        TIME_SECONDS = 30;
    }

    public void start() throws Exception {
        startMongoThreads();
        startStreamThread();
        startRateTrackingThread();
    }

    private void startMongoThreads() {
        mongoConnector.connect();
        int writerCount = Integer.parseInt(properties.getProperty("mongoWriterCount"));

        if ((null == mongoExecutorService) || (mongoExecutorService.isShutdown())) {
            mongoExecutorService = Executors.newFixedThreadPool(writerCount);
            for(int i = 0; i < writerCount; i++) {
                mongoExecutorService.submit(createMongoRunnable());
            }
        }
    }

    private void startStreamThread() throws Exception {
        if ((null == executorService) || (executorService.isShutdown())) {
            executorService = Executors.newSingleThreadExecutor();
            executorService.submit(createStreamRunnable());
        }
    }

    private void startRateTrackingThread() {
        if ((null == rateTrackingExecutorService) || (rateTrackingExecutorService.isShutdown())) {
            rateTrackingExecutorService = Executors.newSingleThreadScheduledExecutor();
            rateTrackingExecutorService.scheduleAtFixedRate(createRateTrackingRunnable(), 0, TIME_SECONDS, TimeUnit.SECONDS);
        }
    }

    private Runnable createMongoRunnable() {
        return new Runnable() {
            @Override
            public void run() {
                MongoWriter mongoWriter = new MongoWriter(properties, mongoConnector, messageQueue);
                mongoWriter.processMessages();
            }
        };
    }

    private Runnable createStreamRunnable() throws Exception {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    connection = getConnection(streamURL, username, password);
                    inputStream = connection.getInputStream();

                    int responseCode = connection.getResponseCode();

                    if (responseCode >= 200 && responseCode <= 299) {
                        BufferedReader reader = new BufferedReader(new InputStreamReader(new StreamingGZIPInputStream(inputStream), charset));
                        String line = reader.readLine();

                        while(line != null){
    //                      if(currentMessageCount.get() <= saveCount) {
                                messageQueue.add(line);
    //                      }
                            currentMessageCount.incrementAndGet();

                            line = reader.readLine();
                        }
                    } else {
                        handleNonSuccessResponse(connection);
                    }
                } catch (Exception e) {
                    if (connection != null) {
                        handleNonSuccessResponse(connection);
                    }
                } finally {
                    closeInputStream();
                }
            }
        };
    }

    private Runnable createRateTrackingRunnable() {
        return new Runnable() {
            @Override
            public void run() {
                logUpdateStatus();
            }
        };
    }

    private void logUpdateStatus() {
        long current = currentMessageCount.get();
        long old = oldMessageCount.get();

        LOGGER.info(String.format("RATE: totalMessagesSent(%d), messagesInTheLastTimeInterval(%d), ratePerSecond(%d)", current,
                current - old,
                (current - old) / TIME_SECONDS));

        oldMessageCount.set(current);
    }

    private static HttpURLConnection getConnection(String urlString, String username, String password) throws IOException {
        URL url = new URL(urlString);

        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setReadTimeout(1000 * 60 * 60);
        connection.setConnectTimeout(1000 * 10);

        connection.setRequestProperty("Authorization", createAuthHeader(username, password));
        connection.setRequestProperty("Accept-Encoding", "gzip");

        return connection;
    }

    private static String createAuthHeader(String username, String password) throws UnsupportedEncodingException {
        BASE64Encoder encoder = new BASE64Encoder();
        String authToken = username + ":" + password;
        return "Basic " + encoder.encode(authToken.getBytes());
    }

    private static void handleNonSuccessResponse(HttpURLConnection connection) {
        try {
            int responseCode = connection.getResponseCode();
            String responseMessage = connection.getResponseMessage();
            System.out.println("Non-success response: " + responseCode + " -- " + responseMessage);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void closeInputStream() {
        try {
            if (null != inputStream) {
                inputStream.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        stopStreamExecutor();
        stopMongoExecutor();
        stopRateTrackingExecutor();
        logUpdateStatus();
    }

    private void stopStreamExecutor() {
        closeInputStream();

        if (null != executorService && !executorService.isShutdown()) {
            executorService.shutdownNow();
        }

        LOGGER.info("Closed stream.");
    }

    private void stopMongoExecutor() {
        if (null != mongoExecutorService && !mongoExecutorService.isShutdown()) {
            mongoExecutorService.shutdownNow();
        }

        mongoConnector.close();
        LOGGER.info("Closed Mongo connection.");
    }

    private void stopRateTrackingExecutor() {
        if (null != rateTrackingExecutorService && !rateTrackingExecutorService.isShutdown()) {
            rateTrackingExecutorService.shutdownNow();
        }

        LOGGER.info("Stopped rate tracking.");
    }

    public Properties getProperties() {
        return properties;
    }

    private static void printUsage() {
        StringBuffer buf = new StringBuffer();
        buf.append("GnipTwitter POC Usage params: ").append("\n")
                .append("GnipTwitter <POWERTRACK|DECAHOSE> <mongoSaveCount> <runDurationMilliseconds>").append("\n\n")
                .append("POWERTRACK - runs the Gnip Powertrack stream").append("\n")
                .append("DECAHOSE - runs the Gnip Decahose stream").append("\n")
                .append("mongoSaveCount - max docs to be saved in mongodb for this run. (We will not save the entire data in mongodb in any case)").append("\n")
                .append("runDurationMilliseconds - amount of time to run the stream for. Limit this to a couple of hours max on staging.")
                .append("\n");
        System.out.println(buf.toString());
    }


}