# Defining a Partition Key

Partition keys are used in Apache Kafka to determine which partition a message should be written to. This ensures that related messages are kept together in the same partition, which can be useful for ensuring order or for grouping related messages together for efficient processing. Additionally, partitioning data across multiple partitions allows Kafka to distribute load across multiple brokers and scale horizontally, while replicating data across multiple brokers provides fault tolerance.

You can define your partition keys when using the `#!python @KafkaBroker.publisher(...)` decorator. This guide will demonstrate this feature to you.

## Calling `publish` with a Key

To publish a message to a Kafka topic using a key, simply pass the `key` parameter to the `publish` function call, like this:

```python
{!> docs_src/kafka/publish_with_partition_key/app.py [ln:25] !}
```

## App Example

Let's take a look at the whole app example that will consume from the **input_data** topic and publish with a key to the **output_data** topic.

You can see that the only difference from normal publishing is that now we pass the key to the publisher call.

```python linenums="1" hl_lines="25"
{!> docs_src/kafka/publish_with_partition_key/app.py [ln:1-25] !}
```