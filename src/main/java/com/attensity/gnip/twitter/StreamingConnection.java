package com.attensity.gnip.twitter;

import com.attensity.gnip.twitter.configuration.Configuration;
import com.attensity.gnip.twitter.configuration.Configurer;
import com.attensity.mongo.MongoConnector;
import com.attensity.mongo.MongoWriter;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Encoder;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author lmedina
 */
public class StreamingConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingConnection.class);

    private static StreamingConnection streamingConnection = new StreamingConnection();

    private Configurer configurer;
    private Configuration configuration;
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
    private AtomicLong oldMessageCount = new AtomicLong(0);
    private AtomicLong currentMessageCount = new AtomicLong(0);

    public static void main(String... args) throws IOException {
        try {

            String configFileName = args[0];
            if(StringUtils.isEmpty(configFileName)) {
                configFileName = "TwitterPowertrackConfig.json";
            }

            LOGGER.info("Reading configuration file: " + configFileName);
            streamingConnection.init(null, configFileName);
            streamingConnection.start();

            Thread.sleep(streamingConnection.getConfiguration().getRunDurationMilliseconds());

            streamingConnection.stop();
        } catch (Exception e) {
            LOGGER.error("Failed to start daemon.", e);
        }
    }

    public void init(DaemonContext context, String configurationFilePath) throws Exception {
        configurer = new Configurer(configurationFilePath);
        configuration = configurer.getConfig();

        username = "ebradley@attensity.com";
        password = "@tt3ns1ty";
        streamURL = configuration.getGnipUrl();
        charset = "UTF-8";

        messageQueue = new LinkedBlockingQueue<String>();
        mongoConnector = new MongoConnector();

        TIME_SECONDS = 30;
    }

    public void start() throws Exception {
        startMongoThreads();
        startStreamThread();
        startRateTrackingThread();
    }

    private void startMongoThreads() {
        mongoConnector.connect();
        int writerCount = configuration.getMongoWriterCount();

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
                MongoWriter mongoWriter = new MongoWriter(configuration, mongoConnector, messageQueue);
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
                            messageQueue.add(line);
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
                    if (inputStream != null) {
                        closeInputStream();
                    }
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

        LOGGER.info(String.format("RATES: totalMessagesSent(%d), messagesInTheLastTimeInterval(%d), ratePerSecond(%d)", current,
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
            inputStream.close();
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

    public Configuration getConfiguration() {
        return configuration;
    }

    public void destroy() {
        LOGGER.info("Done.");
    }
}