# Apache Flink DynamoDB Connectors

This is currently written mainly for Apache Flink 1.13.2, 
the version used in AWS Kinesis Data Analytics.

Newer Flink releases has `org.apache.flink.api.connector.sink2.Sink` interface
that is not yet implemented here.

## Getting Started
```shell
# compile
sbt compile
# running tests
sbt test 
```

## Knowledge
See NOTES.md

## TODO
DynamoDbSyncSink (DataStream API only)
- [x] implement DynamoDbUnifiedSink
- [x] use Flink State interface instead my own `HashSet<In> buffer`
- [x] checkpointing
- [ ] checkpointing test

Unified sink (Batch and Stream)
- [ ] checkpointing
- [ ] error handling (what to do after 3 failures)

DynamoDbAsyncSink (DataStream API only)
- [ ] implement
- [ ] checkpointing