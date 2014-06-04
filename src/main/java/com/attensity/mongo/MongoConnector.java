package com.attensity.mongo;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;

/**
 * This class is responsible for maintaining connection to MongoDB instance.
 * @author pgangrade
 */
public class MongoConnector {
    private Logger LOGGER = LoggerFactory.getLogger(MongoConnector.class);
    private MongoClient mongoClient;
    private String host;
    private int port;
    private String databaseName;

    public MongoConnector() {
        host = "127.0.0.1";
        port = 27017;
        databaseName = "gnip";
    }

    /**
     * Establishes a connection to the specified MongoDB instance, if not already connected.
     *
     * @throws java.net.UnknownHostException if the MongoDB host could not be determined
     */
    public void connect() {
        if (null == mongoClient) {
            try {
                mongoClient = new MongoClient(host, port);
                LOGGER.info(String.format("Connected to MongoDB instance at %s:%d", host, port));
                verifyConnectedToMongoDBDatabase();
            } catch (UnknownHostException e) {
                LOGGER.error(String.format("Unable to connect to MongoDB host, %s, could not be determined, exception: %s", host, e.getMessage()));
                throw new RuntimeException(e);
            } catch (MongoException e) {
                LOGGER.error(String.format("Unable to connect to MongoDB at host:%s, port:%d, exception: %s", host, port, e.getMessage()));
                throw new IllegalStateException(e);
            }
        } else {
            LOGGER.warn(String.format("Attempted connection to MongoDB instance at %s:%d, when already connected.", host, port));
        }
    }

    /**
     * Closes MongoDB connection.
     */
    public void close() {
        try {
            if (null != mongoClient) {
                mongoClient.close();
                mongoClient = null;
                LOGGER.info(String.format("Closed connection to MongoDB instance at %s:%d", host, port));
            }
        } catch(RuntimeException e) {
            LOGGER.error("Unable to close connection to Mongodb", e);
        }
    }

    /**
     * Returns connection to specified database-name
     * @return
     */
    DB getDatabase() {
        verifyConnectedToMongoDBDatabase();
        return mongoClient.getDB(databaseName);
    }

    boolean verifyConnectedToMongoDBDatabase() {
        if (null == mongoClient) {
            throw new IllegalStateException(String.format("Failed to determine database \"%s\". MongoConnector.connect() may not have been invoked.", databaseName));
        }
        if (!mongoClient.getDatabaseNames().contains(databaseName)) {
            throw new IllegalStateException(String.format("Failed to determine database \"%s\" on host \"%s\", port \"%d\".", databaseName, host, port));
        }
        return true;
    }
}
