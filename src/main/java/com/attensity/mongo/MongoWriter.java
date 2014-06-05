package com.attensity.mongo;

import com.attensity.gnip.twitter.configuration.Configuration;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.MongoException;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class MongoWriter {
    private Logger LOGGER = LoggerFactory.getLogger(MongoWriter.class);

    private DBCollection mongoCollection;
    private BlockingQueue<String> messageQueue;

    private long messagesNotStored;

    private ObjectMapper mapper;

    public MongoWriter(Configuration configuration, MongoConnector mongoConnector, BlockingQueue<String> messageQueue) {
        this.mongoCollection = mongoConnector.getDatabase().getCollection(configuration.getMongoCollectionName());

        this.messageQueue = messageQueue;
        this.mapper = new ObjectMapper();
    }

    public void processMessages() {
        try {
            while (true) {
                String message = messageQueue.take();

                if (StringUtils.isNotBlank(message)) {
                    Map<String, Object> messageMap = createMessageMap(message);

                    if (null != messageMap && messageMap.get("objectType").equals("activity") && (messageMap.get("verb").equals("post") || messageMap.get("verb").equals("share"))) {
                        insertIntoMongo(messageMap);
                    } else {
                        System.out.println("Dropped");
                    }
                }
            }
        } catch (InterruptedException e) {
            LOGGER.warn("Interrupted while reading from messageQueue.");
        }
    }

    private Map<String, Object> createMessageMap(String message) {
        try {
            return mapper.readValue(message.getBytes(), new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("ERROR with message - " + message);
            return null;
        }
    }

    public void insertIntoMongo(Map<String, Object> messageMap) {
        try {
            mongoCollection.insert(new BasicDBObject(messageMap));
        } catch (MongoException e) {
            messagesNotStored++;

            if (messagesNotStored % 20 == 0) {
                LOGGER.info(String.format("Unable to insert into Mongo. (Total messages not stored = %d)", messagesNotStored));
            }
        }

    }
}