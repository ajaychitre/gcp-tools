package com.dilteam.dataflow;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
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
import org.apache.beam.sdk.values.PCollection;
import twitter4j.Place;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class TweetsWriter {
//    private static Logger LOGGER = LoggerFactory.getLogger(TweetsWriter.class);

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
        fields.add((new TableFieldSchema()).setName("UserId").setType("INTEGER"));
        fields.add((new TableFieldSchema()).setName("CountryCode").setType("STRING"));
        fields.add((new TableFieldSchema()).setName("Country").setType("STRING"));
        fields.add((new TableFieldSchema()).setName("ReceivedAt").setType("STRING"));
        TableSchema schema = (new TableSchema()).setFields(fields);

        PCollection<Status> statuses = p
                .apply("GetTweets", PubsubIO.readStrings().fromTopic(topic))
                .apply("ExtractData", ParDo.of(new DoFn<String, Status>() {
            @ProcessElement
            public void processElement(DoFn<String, Status>.ProcessContext c) throws Exception {
                    String rowJson = c.element();

                    try {
//                        TweetsWriter.LOGGER.debug("ROWJSON = " + rowJson);
                        Status status = TwitterObjectFactory.createStatus(rowJson);
                        if (status == null) {
//                            TweetsWriter.LOGGER.error("Status is null");
                        } else {
//                            TweetsWriter.LOGGER.debug("Status value: " + status.getText());
                        }
                        c.output(status);
//                        TweetsWriter.LOGGER.debug("Status: " + status.getId());
                    } catch (Exception var4) {
//                        TweetsWriter.LOGGER.error("Status creation from JSON failed: " + var4.getMessage());
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
                        row.set("RetweetCount", Optional.of(status.getRetweetCount()).orElse(0));
                        row.set("FavoriteCount", Optional.of(status.getFavoriteCount()).orElse(0));
                        row.set("Language", Optional.of(status.getLang()).orElse("Unknown"));
                        row.set("UserId", Optional.of(status.getUser().getId()).orElse(0l));

                        String countryCode = Optional.ofNullable(status.getPlace())
                                .map(Place::getCountryCode)
                                .orElse("Unknown");
                        row.set("CountryCode", countryCode);

                        String country = Optional.ofNullable(status.getPlace())
                                .map(Place::getCountry)
                                .orElse("Unknown");
                        row.set("Country", country);
                        row.set("ReceivedAt", null);
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
