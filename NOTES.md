# Apache Flink DynamoDB Connectors Notes

## Checkpointing

Why?
- We have `DynamoDbSyncSink` for example, when do we need to implement `CheckpointedFunction`?
    - Because checkpoints is for making state in Flink fault-tolerant, we need to
    implement it only if our function is stateful.
    - Note that checkpoint is a global operation triggered by source nodes to downstream nodes.
    
- What if we don't?
    - Let's say `DynamoDbSyncSink` uses `DynamoDbBatchingOutputFormat` which in turn maintains
      a state of the records buffer it received from the sink to do batch writing to the DB.
    - Transient Failure: If the streaming app crashes while a buffer has unwritten record(s), without checkpointing,
      upon restarting the buffer will be empty, and we lost the record. One example of transient
      failure is DynamoDb service is temporarily unavailable.
    - Code Update: Also, in case the code/ logic of your streaming app needs to be updated, in flight records
    and in the buffer will be lost when you restart your app to a new JAR.

- As checkpoint is a global operation triggered by source nodes to downstream nodes, what happens 
  if the sink does not implement the checkpoint functionality (e.g. extends `RichSinkFunction<In>`
  without implements `CheckpointedFunction`)?

How?
- Because there are always events in-flight (on the network, in I/O buffers, etc.), 
  up- and downstream operators can be processing events from different times: 
  the sink may write data from 11:04, while the source already ingests events from 11:06
- Flink injects checkpoint barriers into the streams at the sources, 
  which travel through the entire topology and eventually reach the sinks.
- These barriers divide the stream into a pre-checkpoint epoch (all events that are persisted 
  in state or emitted into sinks) and a post-checkpoint epoch (events not reflected in the state, 
  to be re-processed when resuming from the checkpoint)

When will Flink consider a checkpoint complete?
- Once all the tasks (including the sinks) have reported back to the checkpoint coordinator 
  that they have finishing writing out a snapshot of their state, the checkpoint coordinator 
  will write the checkpoint metadata, and then notify all participants that the checkpoint is complete.

Example of a simple pipeline 2 operators: SourceOperator and SinkOperator
```
|B| stands for checkpoing barrier

<---time----
Kafka -> 4 3 |B| 2 1 -> SourceOperator -> SinkOperator -> DataStore ()
(1) Flink injects checkpoint barrier into stream at source.

Kafka -> 4 3 |B| -> SourceOperator -> 2 1 > SinkOperator -> DataStore ()
(2) SourceOperator receives barrier and snapshots its state, forward barrier to output.

Kafka -> 4 -> SourceOperator -> 3 |B| > SinkOperator -> DataStore (2 1)
(3) SinkOperator receives barrier and snapshots its state, forward barrier to output.

Kafka -> 4 3 -> SourceOperator -> 4 3 > SinkOperator -> DataStore (2 1)
(4) SourceOperator and SinkOperator finished writing their state snapshot to checkpoint storage
and report to CheckpointCoordinator.
(5) CheckpointCoordinator writes checkpoint metadata and notify all partitipants
that checkpoint is complete and can be used for recovery.

Kafka -> 5 |B| 4 3 -> SourceOperator -> 4 3 > SinkOperator -> DataStore (2 1)
(6) Pipeline continues running until a new checkpoint barrier is injected after record 4
and the process repeat.

```

## State
- In `DynamoDbBatchingOutputFormat<In>`, using `transient HashSet<In> buffer` vs 
  `transient ListState<In> buffer` as state? In other words, what is the difference in using
  Flink state API vs using your own data structure like a HashSet?


## Reference
- [Flink Checkpoints Principles and Practices: Flink Advanced Tutorials](https://www.alibabacloud.com/blog/flink-checkpoints-principles-and-practices-flink-advanced-tutorials_596631)
- [From Aligned to Unaligned Checkpoints Part 1](https://flink.apache.org/2020/10/15/from-aligned-to-unaligned-checkpoints-part-1.html)
- [When will Flink consider a checkpoint complete](https://stackoverflow.com/questions/67294690/when-will-flink-consider-a-checkpoint-complete-before-or-after-the-sink-functio)