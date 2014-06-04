package com.attensity.gnip.twitter;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Encoder;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @author lmedina
 */
public class StreamingConnection implements Daemon {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingConnection.class);

    private static StreamingConnection streamingConnection = new StreamingConnection();

    private String username;
    private String password;
    private String streamURL;
    private String charset;

    private HttpURLConnection connection;
    private InputStream inputStream;

    public static void main(String... args) throws IOException {
        try {
            streamingConnection.init(null);
            streamingConnection.start();
        } catch (Exception e) {
            LOGGER.error("Failed to start daemon.", e);
        }
    }

    @Override
    public void init(DaemonContext context) throws Exception {
        username = "ebradley@attensity.com";
        password = "@tt3ns1ty";
        streamURL = "https://stream.gnip.com:443/accounts/Attensity/publishers/twitter/streams/track/prod.json";
        charset = "UTF-8";
    }

    @Override
    public void start() throws Exception {
        try {
            connection = getConnection(streamURL, username, password);
            inputStream = connection.getInputStream();

            int responseCode = connection.getResponseCode();

            if (responseCode >= 200 && responseCode <= 299) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(new StreamingGZIPInputStream(inputStream), charset));
                String line = reader.readLine();

                while(line != null){
                    System.out.println(line);
                    line = reader.readLine();
                }
            } else {
                handleNonSuccessResponse(connection);
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (connection != null) {
                handleNonSuccessResponse(connection);
            }
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }

    private static void handleNonSuccessResponse(HttpURLConnection connection) throws IOException {
        int responseCode = connection.getResponseCode();
        String responseMessage = connection.getResponseMessage();
        System.out.println("Non-success response: " + responseCode + " -- " + responseMessage);
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

    @Override
    public void stop() throws Exception {
        inputStream.close();
        System.out.println("Closed InputStream.");
    }

    @Override
    public void destroy() {

    }
}