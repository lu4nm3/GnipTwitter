package com.attensity.gnip.twitter.configuration;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class Configurer {
    private Logger LOGGER = LoggerFactory.getLogger(Configurer.class);

    public static final String DEFAULT_CONFIG_NAME = "TwitterPowertrackConfig.json";
    protected static ObjectMapper mapper = new ObjectMapper();

    private Configuration configuration;

    public Configurer(String fileName) {
        this.configuration = buildConfiguration(readConfigFromFile(fileName));
    }

    public Configuration buildConfiguration(String rawConfigurationInJson){
        try{
            LOGGER.info(rawConfigurationInJson);
            return mapper.readValue(rawConfigurationInJson,Configuration.class);
        }catch(JsonMappingException jme){
            LOGGER.error("Json Configuration could not be mapped to java classes properly", jme);
        }catch(JsonParseException jpe){
            LOGGER.error("Json could not be parsed properly, rawString: " + rawConfigurationInJson, jpe);
        }catch(IOException ioe){
            LOGGER.error(ioe.getMessage(), ioe);
        }
        return null;
    }

    public Configuration getConfig() {
        return configuration;
    }
    protected String readConfigFromFile(String filePath) {
        if(null == filePath){
            filePath = DEFAULT_CONFIG_NAME;
        }
        File config = new File(filePath);
        InputStream fis=null;
        BufferedInputStream bis = null;
        DataInputStream dis = null;
        StringBuffer rawConfiguration = new StringBuffer();

        try{
            String q;
            if (config.exists()) {
                fis = new FileInputStream(config);
            }
            else {
                LOGGER.error("config could not be read from " + filePath);
                throw new RuntimeException("config could not be read from " + filePath);
            }
            bis = new BufferedInputStream(fis);
            dis = new DataInputStream(bis);
            q = "";

            while(dis.available() != 0){
                q = dis.readLine();
                rawConfiguration.append(q);
            }
            fis.close();
            bis.close();
            dis.close();
        }catch(FileNotFoundException e){
            LOGGER.error(e.getMessage(), e);
        }catch(IOException e){
            LOGGER.error(e.getMessage(),e);
        }
        LOGGER.info(rawConfiguration.toString());
        return rawConfiguration.toString();
    }}