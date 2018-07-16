package com.dilteam.restServer.coreApp;

import com.dilteam.twitter.TweetsPublisher;
import com.dilteam.twitter.TwitterStatusListener;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.PropertySources;
import org.springframework.web.bind.annotation.CrossOrigin;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.io.IOException;

@SpringBootApplication
@EnableAutoConfiguration
@ComponentScan(basePackages = {"com.dilteam"})
@PropertySources({@PropertySource(value = "classpath:application.properties")})
@CrossOrigin(allowedHeaders = "*", allowCredentials = "true")
public class RestServer {

    @Value("${authConsumerKey}")
    private String authConsumerKey;

    @Value("${authConsumerSecret}")
    private String authConsumerSecret;

    @Value("${authAccessToken}")
    private String authAccessToken;

    @Value("${authAccessTokenSecret}")
    private String authAccessTokenSecret;

    @Value("${googleApplicationCredentials}")
    private String googleApplicationCredentials;

    @Value("${batchSize}")
    public Integer batchSize;

    @Value("${bigQueryDataset}")
    public String bigQueryDataset;

    @Value("${bigQueryTable}")
    public String bigQueryTable;

    @Value("${gcpProjectId}")
    public String gcpProjectId;

    @Value("${topicId}")
    public String topicId;

    @Value("${streamingType}")
    public String streamingType;

    public static void main(String[] args) throws Exception {
        SpringApplication.run(RestServer.class);
    }

    @Bean
    public ConfigurationBuilder getConfigurationBuilder() {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey(authConsumerKey)
                .setOAuthConsumerSecret(authConsumerSecret)
                .setOAuthAccessToken(authAccessToken)
                .setOAuthAccessTokenSecret(authAccessTokenSecret);
        cb.setJSONStoreEnabled(true);
        return cb;
    }

    @Bean
    public BigQuery getBigQuery() {
        System.setProperty("GOOGLE_APPLICATION_CREDENTIALS", googleApplicationCredentials);
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
        return bigquery;
    }

    @Bean
    public TwitterStatusListener getTwitterStatusListener() {
        return new TwitterStatusListener(batchSize, gcpProjectId,bigQueryDataset, bigQueryTable);
    }

    @Bean
    public TweetsPublisher getTweetsPublisher() throws IOException {
        TweetsPublisher publisher = new TweetsPublisher(batchSize, gcpProjectId, topicId);
        return publisher;
    }

    @Bean
    public TwitterStream getTwitterStream() throws IOException {
        TwitterStream twitterStream = new TwitterStreamFactory(getConfigurationBuilder().build()).getInstance();
        if (streamingType.equalsIgnoreCase("DirectIntoBiqquery")) {
            twitterStream.addListener(getTwitterStatusListener());
        } else {
            twitterStream.addListener(getTweetsPublisher());
        }
        twitterStream.sample();
        return twitterStream;
    }

}
