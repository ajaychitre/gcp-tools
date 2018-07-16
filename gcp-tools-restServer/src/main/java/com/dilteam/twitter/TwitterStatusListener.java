package com.dilteam.twitter;

import com.google.cloud.bigquery.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.stereotype.Component;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@EnableAutoConfiguration
public class TwitterStatusListener implements StatusListener {
    private static Logger LOGGER = LoggerFactory.getLogger(TwitterStatusListener.class);

    private static int counter = 0;
    private Integer batchSize;

    @Autowired
    public BigQuery bigQuery;

    private TableId tableId = null;
    private InsertAllRequest.Builder insertAllRequestBuilder = null;

    @Autowired
    public TwitterStatusListener(@Value("${batchSize}") Integer batchSize,
                                 @Value("${gcpProjectId}") String gcpProjectId,
                                 @Value("${bigQueryDataset}") String bigQueryDataset,
                                 @Value("${bigQueryTable}") String bigQueryTable
                                 ) {
        tableId = TableId.of(gcpProjectId, bigQueryDataset, bigQueryTable);
        insertAllRequestBuilder = InsertAllRequest.newBuilder(tableId);
        this.batchSize = batchSize;
    }

    @Override
    public void onStatus(Status status) {
        if (counter == batchSize) {
            long started = System.currentTimeMillis();
            LOGGER.info("Started...");
            InsertAllResponse response = bigQuery.insertAll(insertAllRequestBuilder.build());
            LOGGER.info("Ended...");
            long ended = System.currentTimeMillis();
            LOGGER.info("Total time taken to insert {} rows: {} seconds" , batchSize, (ended - started) / 1000);

            if (response.hasErrors()) {
                // If any of the insertions failed, this lets you inspect the errors
                for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                    LOGGER.info("Error: Key: {} Value: {}", entry.getKey(), entry.getValue().toString());
                }
            }
            LOGGER.info("$$$$$$  Total no. of tweets inserted: {} at: {}", counter,
                    new Date(System.currentTimeMillis()).toString());
            insertAllRequestBuilder = null;
            insertAllRequestBuilder = InsertAllRequest.newBuilder(tableId);
            counter = 0;
        }

        if (status.getLang() != null && status.getLang().equalsIgnoreCase("en")) {
            Map<String, Object> rowContent = new HashMap<>();
            rowContent.put("Id", status.getId());
            rowContent.put("Text", status.getText());
            rowContent.put("RetweetCount", status.getRetweetCount());
            rowContent.put("FavoriteCount", status.getFavoriteCount());
            rowContent.put("Language", status.getLang());
            rowContent.put("ReceivedAt", null);
            insertAllRequestBuilder.addRow(rowContent);
            counter++;
        }
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
