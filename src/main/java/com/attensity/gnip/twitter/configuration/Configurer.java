package com.attensity.gnip.twitter.configuration;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URISyntaxException;

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
        StringBuffer rawConfiguration = new StringBuffer();
        String line;

        if(null == filePath){
            filePath = DEFAULT_CONFIG_NAME;
        }

        File file = null;
        try {
            file = new File(Configurer.class.getResource("/" + filePath).toURI());
            BufferedReader br = new BufferedReader(new FileReader(file));
            while ((line = br.readLine()) != null) {
                rawConfiguration.append(line);
            }
            br.close();

        } catch (URISyntaxException | IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
        LOGGER.info(rawConfiguration.toString());
        return rawConfiguration.toString();
    }}