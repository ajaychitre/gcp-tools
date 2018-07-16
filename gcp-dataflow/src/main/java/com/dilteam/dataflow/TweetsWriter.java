package com.dilteam.dataflow;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;

public class TweetsWriter {
    private static Logger LOGGER = LoggerFactory.getLogger(TweetsWriter.class);

    public TweetsWriter() {
    }

    public static void main(String[] args) {
        DataflowPipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(DataflowPipelineOptions.class);
        options.setStreaming(true);

        Pipeline p = Pipeline.create(options);

        String topic = "projects/" + options.getProject() + "/topics/ajay-topic-1";
        String tweetsTable = options.getProject() + ":TweetsAnalyzer.Tweets_DataFlow";

        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add((new TableFieldSchema()).setName("Id").setType("INTEGER"));
        fields.add((new TableFieldSchema()).setName("Text").setType("STRING"));
        fields.add((new TableFieldSchema()).setName("RetweetCount").setType("INTEGER"));
        fields.add((new TableFieldSchema()).setName("FavoriteCount").setType("INTEGER"));
        fields.add((new TableFieldSchema()).setName("Language").setType("STRING"));
        fields.add((new TableFieldSchema()).setName("ReceivedAt").setType("DATETIME"));
        fields.add((new TableFieldSchema()).setName("UserId").setType("INTEGER"));
        fields.add((new TableFieldSchema()).setName("CountryCode").setType("STRING"));
        fields.add((new TableFieldSchema()).setName("Country").setType("STRING"));
        TableSchema schema = (new TableSchema()).setFields(fields);

        PCollection<Status> statuses = p
                .apply("GetTweets", PubsubIO.readStrings().fromTopic(topic))
                .apply("ExtractData", ParDo.of(new DoFn<String, Status>() {
            @ProcessElement
            public void processElement(DoFn<String, Status>.ProcessContext c) throws Exception {
                    String rowJson = c.element();

                    try {
                        TweetsWriter.LOGGER.debug("ROWJSON = " + rowJson);
                        Status status = TwitterObjectFactory.createStatus(rowJson);
                        if (status == null) {
                            TweetsWriter.LOGGER.error("Status is null");
                        } else {
                            TweetsWriter.LOGGER.debug("Status value: " + status.getText());
                        }
                        c.output(status);
                        TweetsWriter.LOGGER.debug("Status: " + status.getId());
                    } catch (Exception var4) {
                        TweetsWriter.LOGGER.error("Status creation from JSON failed: " + var4.getMessage());
                    }

            }
        }));

        statuses
                .apply("ToBQRow", ParDo.of(new DoFn<Status, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws Exception {
                        TableRow row = new TableRow();
                        Status status = c.element();
                        row.set("Id", status.getId());
                        row.set("Text", status.getText());
                        row.set("RetweetCount", status.getRetweetCount());
                        row.set("FavoriteCount", status.getFavoriteCount());
                        row.set("Language", status.getLang());
                        row.set("ReceivedAt", (Object)null);
                        row.set("UserId", status.getUser().getId());
                        row.set("CountryCode", status.getPlace().getCountryCode());
                        row.set("Country", status.getPlace().getCountry());
                        c.output(row);
                }
            }))
                .apply("WriteTableRows", BigQueryIO.writeTableRows().to(tweetsTable)
                        .withSchema(schema)
                        .withMethod(Method.STREAMING_INSERTS)
                        .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));

        p.run();
    }
}
