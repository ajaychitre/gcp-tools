# gcp-tools

## Streaming data directly into BigQuery

Follow the steps given below to stream data directly into BigQuery:

1. Create an App at: https://apps.twitter.com/
2. Twitter will then provide you values for the following 4 properties which you can use to get a 'Sample' of Tweets in real time:
```$xslt
authConsumerKey
authConsumerSecret
authAccessToken
authAccessTokenSecret
```
3. You can enter values of the above 4 properties in  [application.properties](./gcp-tools-restServer/src/main/resources/application.properties) along with values of the following properties:
```$xslt
streamingType=DirectIntoBigQuery
gcpProjectId=<Your project Id>
biqQueryDataset=<Dataset>
bigQueryTable=<Table>
```
4. Go to [BigQuery Console](https://bigquery.cloud.google.com/) and create the **dataset** & **table** that you specified in the previous step.
5. Under GCP, create a [Service Account](https://cloud.google.com/iam/docs/creating-managing-service-account-keys) & put the JSON file somewhere in your local machine.
6. Add an enviroment variable called, **GOOGLE_APPLICATION_CREDENTIALS** & set the value to the file location of the JSON created in the previous step.
7. Compile the source code:
```$xslt
mvn clean install
```
8. Start the 'RestServer' class either from command line or from your favorite IDE such as IntelliJ. This will automatically start streaming tweets into your table in BigQuery.


