<h3>Description</h3>
The project is created for keeping Flink samples (especially testing). 

<h3>Versions</h3>
JDK 11, Gradle 7.5.1, Flink 1.17.0

<h3>Modules</h3>

- `flink-simple` User and Action are merged into UserAction by userId key;<br>The module has Kafka sink and source;  smoke, integration and unit tests are present.
- `flink-broadcast` Action field is filtered by Rule. Rule contains a list of allowed action names. Rule data stream is broadcasted;<br>Sink and source are created in tests; integration and unit tests are present.
- `flink-table` repeats flink-simple project, but with using Table API 
- `kafka-tools` consumer and producer
