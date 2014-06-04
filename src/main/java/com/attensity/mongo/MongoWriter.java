package com.attensity.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class MongoWriter {
    private Logger logger = LoggerFactory.getLogger(MongoWriter.class);
    private MongoConnector mongoConnector;
    //    private Clock clock = new SystemClock();
    private DB database;
    private boolean shutdown = false;

    static final SimpleDateFormat DAY_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    static final TimeZone TIMEZONE = TimeZone.getTimeZone("GMT");

    static {
        DAY_FORMAT.setTimeZone(TIMEZONE);
        DATE_FORMAT.setTimeZone(TIMEZONE);
    }

    public MongoWriter(MongoConnector mongoConnector) {
        this.mongoConnector = mongoConnector;
//        this.mongoConnector.connect();
//        database = this.mongoConnector.getDatabase();
        logger.info("MongoStorageWriter initialized");
    }

//    public void processMessage(Activity activity) {
//        Map<String, Object> activityMap = null;
//        String collectionName = "twitter";
//        try {
//            activityMap = getActivityMap(activity);
////            insertArticleIntoMongo(activityMap, collectionName);
//        }
//        catch (MongoException e) {
//            retryAttempt(activityMap, collectionName);
//        }
//        catch (RuntimeException e) {
//            logger.error(String.format("Unable to write the activityMap to mongo collection, collection: %s", collectionName), e);
//        }
//    }

    public void processMessage(String activity) {
        Map<String, Object> activityMap = null;
        String collectionName = "twitter";
        try {
            activityMap = getActivityMap(activity);
//            insertArticleIntoMongo(activityMap, collectionName);
        }
        catch (MongoException e) {
            retryAttempt(activityMap, collectionName);
        }
        catch (RuntimeException e) {
            logger.error(String.format("Unable to write the activityMap to mongo collection, collection: %s", collectionName), e);
        }
    }

    private void insertArticleIntoMongo(Map<String, Object> article, String collectionName) {
        DBCollection mongoCollection = database.getCollection(collectionName);
        mongoCollection.insert(new BasicDBObject(article));
    }

    private void retryAttempt(Map<String, Object> article, String collectionName) {
        boolean firstTimeLogging = true;
        while(!shutdown) {
            try {
                if(firstTimeLogging) {
                    firstTimeLogging = false;
                    logger.warn("Retrying save of articleId: " + article.get("id"));
                }
                if (!connectedToDatabase()) {
                    attemptReconnect();
                }

                insertArticleIntoMongo(article, collectionName);
                break; // breakout if insert was successful.
            }
            catch (MongoException e) {
                logger.error(String.format("Unable to write to mongo, collection: %s, exception: %s", collectionName, e.getMessage()));
            }
            catch (RuntimeException e) {
                logger.error(String.format("Unable to write to mongo, collection: %s", collectionName), e);
                break;
            }
        }
    }

    private boolean connectedToDatabase() {
        return mongoConnector.verifyConnectedToMongoDBDatabase();
    }

    private void attemptReconnect() {
        mongoConnector.close();
        mongoConnector.connect();
    }

//    Map<String, Object> getActivityMap(Activity activity) {
//        Map<String, Object> articleMap = new HashMap<String, Object>();
//
//        System.out.println(activity.getPayload());
//
//        return articleMap;
//    }

    Map<String, Object> getActivityMap(String activity) {
        Map<String, Object> articleMap = new HashMap<>();

        System.out.println(activity);

        return articleMap;
    }
}