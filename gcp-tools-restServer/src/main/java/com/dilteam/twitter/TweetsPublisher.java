package com.dilteam.twitter;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.stereotype.Component;
import twitter4j.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Component
@EnableAutoConfiguration
public class TweetsPublisher implements StatusListener {
    private static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(TweetsPublisher.class);

    private int counter = 0;
    private int batchSize;
    private String gcpProjectId;
    private String topicId;
    ProjectTopicName topicName;
    Publisher publisher;
    List<ApiFuture<String>> futures = new ArrayList<>();

    @Autowired
    public TweetsPublisher(@Value("${batchSize}") Integer batchSize,
                           @Value("${gcpProjectId}") String gcpProjectId,
                           @Value("${topicId}") String topicId
                                 ) throws IOException {
        this.batchSize = batchSize;
        this.gcpProjectId = gcpProjectId;
        this.topicId = topicId;
        topicName = ProjectTopicName.of(gcpProjectId, topicId);
        publisher = Publisher.newBuilder(topicName).build();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                LOGGER.info("Inside shutdown hook! Shuttng down Publisher!");
                try {
                    // Wait on any pending requests
                    ApiFutures.allAsList(futures).get();
                    publisher.shutdown();
                    LOGGER.info("Publisher was shut down successfully!");
                } catch (Exception e) {
                    LOGGER.info("Error encountered while shutting down: " + e.getMessage());
                }
            }
        });
    }

    @Override
    public void onStatus(Status status) {
        if (status.getLang() == null || !status.getLang().equalsIgnoreCase("en")) {
            return;
        }
        if (++counter % 100 == 0) {
            LOGGER.info("$$$$$$  Total no. of tweets so far: " + counter + " at: "
                    + new Date(System.currentTimeMillis()).toString());
        }

        String message = TwitterObjectFactory.getRawJSON(status);

        // Convert message to bytes
        ByteString data = ByteString.copyFromUtf8(message);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

        // Schedule a message to be published. Messages are automatically batched.
        ApiFuture<String> future = publisher.publish(pubsubMessage);
        futures.add(future);
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
//        LOGGER.info("onDeletionNotice: " + statusDeletionNotice.toString());
    }

    @Override
    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
        LOGGER.info("onTrackLimitationNotice: " + numberOfLimitedStatuses);
    }

    @Override
    public void onScrubGeo(long userId, long upToStatusId) {
        LOGGER.info("onScrubGeo: " + userId + "\t" + upToStatusId);
    }

    @Override
    public void onStallWarning(StallWarning warning) {
        LOGGER.info("onStallWarning: " + warning.toString());
    }

    @Override
    public void onException(Exception ex) {
        LOGGER.info("onException: " + ex.getMessage());
    }
}
