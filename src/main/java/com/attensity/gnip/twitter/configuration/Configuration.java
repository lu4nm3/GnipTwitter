package com.attensity.gnip.twitter.configuration;

/**
 * @author lgopalan
 */
public class Configuration {
    private String gnipUrl;
    private String gnipUser;
    private String gnipPassword;
    
    private int mongoWriterCount;
    private String mongoCollectionName;
    private int runDurationMilliseconds;

    String DEFAULT_GNIP_USER = "ebradley@attensity.com";
    String DEFAULT_GNIP_PASSWORD = "@tt3ns1ty";
    
    public void setGnipUrl(String url) {
        this.gnipUrl = url;
    }

    public String getGnipUrl() {
        return gnipUrl;
    }

    public void setGnipUser(String user) {
        this.gnipUser = user;
    }

    public String getGnipUser() {
        if(gnipUser == null) {
            return DEFAULT_GNIP_USER;
        }
        return gnipUser;
    }

    public void setGnipPassword(String password) {
        this.gnipPassword = password;
    }

    public String getGnipPassword() {
        if (gnipPassword == null) {
            return DEFAULT_GNIP_PASSWORD;
        }
        return gnipPassword;
    }

    public int getMongoWriterCount() {
        return mongoWriterCount;
    }

    public void setMongoWriterCount(int mongoWriterCount) {
        this.mongoWriterCount = mongoWriterCount;
    }

    public String getMongoCollectionName() {
        return mongoCollectionName;
    }

    public void setMongoCollectionName(String mongoCollectionName) {
        this.mongoCollectionName = mongoCollectionName;
    }

    public int getRunDurationMilliseconds() {
        return runDurationMilliseconds;
    }

    public void setRunDurationMilliseconds(int runDurationMilliseconds) {
        this.runDurationMilliseconds = runDurationMilliseconds;
    }
}
