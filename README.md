# gcp-tools

## Streaming data directly into BigQuery

Follow the steps given below to stream data directly into BigQuery:

1. Create an App at: https://apps.twitter.com/
2. Twitter will then provide you values for the following 4 properties which you can use to get a 'Sample' of Tweets in real time:
```
authConsumerKey
authConsumerSecret
authAccessToken
authAccessTokenSecret
```
3. You can enter values of the above 4 properties in  [application.properties](./gcp-tools-restServer/src/main/resources/application.properties) along with values of the following properties:
```
streamingType=DirectIntoBigQuery
gcpProjectId=<Your project Id>
biqQueryDataset=<Dataset>
bigQueryTable=<Table>
```
4. Go to [BigQuery Console](https://bigquery.cloud.google.com/) and create the **dataset** & **table** that you specified in the previous step.
5. Under GCP, create a [Service Account](https://cloud.google.com/iam/docs/creating-managing-service-account-keys) & put the JSON file somewhere in your local machine.
6. Add an enviroment variable called, **GOOGLE_APPLICATION_CREDENTIALS** & set the value to the file location of the JSON created in the previous step.
7. Compile the source code:
```
mvn clean install
```
8. Start the 'RestServer' class either from command line or from your favorite IDE such as IntelliJ. This will automatically start streaming tweets into your table in BigQuery.


## Using Dataflow & PubSub to load data into BigQuery

1. Follow the steps given in the previous section EXCEPT in step 3 set **streamingType to PubSub*** & **topicId to <your topic id>***. Once you start the 'RestServer', it will start pumping Tweets into your topic.
2. Go to [gcp-dataflow](./gcp-dataflow) directory & run **mvn clean install**
3. Open a terminal window & run [run_on_cloud.sh](./gcp-dataflow/run_on_cloud.sh) as follows:
```
./run_on_cloud.sh <Your project id> <Your bucket name> TweetsWriter
```
4. When the job gets triggered successfully, you will see a URL that you can navigate to. Look for a message such as: **INFO: To access the Dataflow monitoring console, please navigate to**
