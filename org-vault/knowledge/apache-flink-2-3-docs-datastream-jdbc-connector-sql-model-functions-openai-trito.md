---
title: Apache Flink 2.3 docs — DataStream JDBC connector + SQL model functions (OpenAI/Triton/downloads)
tags: [org, flink, flink-2.3, docs, connectors, datastream, jdbc, models, openai, triton, downloads, reference]
author: adrianb
sessions: []
created: '2026-07-08T04:49:59.413Z'
updated: '2026-07-08T04:49:59.413Z'
importanceScore: 1
---

## Executive Summary

**Executive summary:** Apache Flink 2.3 docs condensation covering the DataStream JDBC connector (at-least-once sink + exactly-once via XA) and the SQL model-function connectors (OpenAI, NVIDIA Triton, optional model downloads). **Why kept:** Completes the connectors section of the Flink 2.3 EN docs corpus (per the standing request to ingest all EN subpages into org memories). Prose kept near-verbatim; code blocks condensed to `[code: <first-line> ... (N chars)]`, tables to `[table: N rows]`, so this serves as a faithful reference. Source URLs preserved per section.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/datastream/jdbc/

# JDBC Connector

This connector provides a sink that writes data to a JDBC database.

To use it, add the following dependency to your project (along with your JDBC driver).

There is no connector (yet) available for Flink version 2.3.

Note that the streaming connectors are currently NOT part of the binary distribution. See how to link with them for cluster execution here.
A driver dependency is also required to connect to a specified database. Please consult your database documentation on how to add the corresponding driver.

## JdbcSink.sink

The JDBC sink provides at-least-once guarantee.
Effectively though, exactly-once can be achieved by crafting upsert SQL statements or idempotent SQL updates.
Configuration goes as follow (see also JdbcSink javadoc).

Java `[code: JdbcSink.sink( ... (256 chars)]`
Python `[code: JdbcSink.sink( ... (191 chars)]`

### SQL DML statement and JDBC statement builder

The sink builds one JDBC prepared statement from a user-provider SQL string, e.g.:
`[code: INSERT INTO some_table field1, field2 values (?, ?) ... (51 chars)]`

It then repeatedly calls a user-provided function to update that prepared statement with each value of the stream, e.g.:
`[code: (preparedStatement, someRecord) -> { ... update here the preparedState ... (108 chars)]`

### JDBC execution options

The SQL DML statements are executed in batches, which can optionally be configured (see JdbcExecutionOptions javadoc).

Java `[code: JdbcExecutionOptions.builder() ... (305 chars)]`
Python `[code: JdbcExecutionOptions.builder() \ ... (136 chars)]`

A JDBC batch is executed as soon as one of the following conditions is true:
- the configured batch interval time is elapsed
- the maximum batch size is reached
- a Flink checkpoint has started

### JDBC connection parameters

The connection to the database is configured with a JdbcConnectionOptions instance. Please see JdbcConnectionOptions javadoc for details.

### Full example

Java `[code: public class JdbcSinkExample { ... (2159 chars)]`
Python `[code: env = StreamExecutionEnvironment.get_execution_environment() ... (1147 chars)]`

## JdbcSink.exactlyOnceSink

Since 1.13, Flink JDBC sink supports exactly-once mode. The implementation relies on the JDBC driver support of XA standard. Most drivers support XA if the database also supports XA (so the driver is usually the same).

To use it, create a sink using exactlyOnceSink() method as above and additionally provide:
- exactly-once options
- execution options
- XA DataSource Supplier

Java `[code: StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecuti ... (1017 chars)]`
Python `[code: Still not supported in Python API. ... (34 chars)]`

NOTE: Some databases only allow a single XA transaction per connection (e.g. PostgreSQL, MySQL). In such cases, please use the following API to construct JdbcExactlyOnceOptions:

Java `[code: JdbcExactlyOnceOptions.builder() ... (78 chars)]`
Python `[code: Still not supported in Python API. ... (34 chars)]`

This will make Flink use a separate connection for every XA transaction. This may require adjusting connection limits. For PostgreSQL and MySQL, this can be done by increasing max_connections.

Furthermore, XA needs to be enabled and/or configured in some databases. For PostgreSQL, you should set max_prepared_transactions to some value greater than zero. For MySQL v8+, you should grant XA_RECOVER_ADMIN to Flink DB user.

ATTENTION: Currently, JdbcSink.exactlyOnceSink can ensure exactly once semantics with JdbcExecutionOptions.maxRetries == 0; otherwise, duplicated results maybe produced.

### XADataSource examples

PostgreSQL XADataSource example:
Java `[code: PGXADataSource xaDataSource = new org.postgresql.xa.PGXADataSource(); ... (203 chars)]`
Python `[code: Still not supported in Python API. ... (34 chars)]`

MySQL XADataSource example:
Java `[code: MysqlXADataSource xaDataSource = new com.mysql.cj.jdbc.MysqlXADataSour ... (196 chars)]`
Python `[code: Still not supported in Python API. ... (34 chars)]`

Oracle XADataSource example:
Java `[code: OracleXADataSource xaDataSource = new oracle.jdbc.xa.OracleXADataSourc ... (183 chars)]`
Python `[code: Still not supported in Python API. ... (34 chars)]`

Please also take Oracle connection pooling into account. Please refer to the JdbcXaSinkFunction documentation for more details.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/models/openai/

# OpenAI

The OpenAI Model Function allows Flink SQL to call OpenAI API for inference tasks.

## Overview

The function supports calling remote OpenAI model services via Flink SQL for prediction/inference tasks. Currently, the following tasks are supported:
- Chat Completions: generate a model response from a list of messages comprising a conversation.
- Embeddings: get a vector representation of a given input that can be easily consumed by machine learning models and algorithms.

## Usage examples

The following example creates a chat completions model and uses it to predict sentiment labels for movie reviews.

First, create the chat completions model with the following SQL statement:
`[code: CREATE MODEL ai_analyze_sentiment ... (380 chars)]`

Suppose the following data is stored in a table named movie_comment, and the prediction result is to be stored in a table named print_sink:
`[code: CREATE TEMPORARY VIEW movie_comment(id, movie_name,  user_comment, act ... (436 chars)]`

Then the following SQL statement can be used to predict sentiment labels for movie reviews:
`[code: INSERT INTO print_sink ... (184 chars)]`

## Model Options

### Common
[table: 9 rows]

### Chat Completions
[table: 10 rows]

### Embeddings
[table: 2 rows]

## Schema Requirement

The following table lists the schema requirement for each task.
[table: 3 rows]

### Available Metadata

When configuring error-handling-strategy as ignore, you can choose to additionally specify the following metadata columns to surface information about failures into your stream.
- error-string(STRING): A message associated with the error
- http-status-code(INT): The HTTP status code
- http-headers-map(MAP<STRING, ARRAY>): The headers returned with the response

If you defined these metadata columns in the output schema but the call did not fail, the columns will be filled with null values.

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/models/triton/

# Triton

## Overview

The Triton Model Function allows Flink SQL to call NVIDIA Triton Inference Server for real-time model inference tasks. Triton Inference Server is a high-performance inference serving solution that supports multiple machine learning frameworks including TensorFlow, PyTorch, ONNX, TensorRT, and more.

Key features:
- High Performance: Optimized for low-latency and high-throughput inference
- Multi-Framework Support: Works with models from various ML frameworks
- Asynchronous Processing: Non-blocking inference requests for better resource utilization
- Flexible Configuration: Comprehensive configuration options for different use cases
- Resource Management: Efficient HTTP client pooling and automatic resource cleanup
- Fault Tolerance: Built-in retry mechanism with configurable attempts

The flink-model-triton module is available since Flink 2.3. Ensure you have access to a running Triton Inference Server instance.

## Usage Examples

### Example 1: Text Classification (Basic)
This example demonstrates sentiment analysis on movie reviews:
SQL `[code: -- Create the Triton model ... (1122 chars)]`
Table API (Java) `[code: TableEnvironment tEnv = TableEnvironment.create(...); ... (1048 chars)]`

### Example 2: Image Classification with Streaming
Classify images from a Kafka stream using a ResNet model:
`[code: -- Register image classification model ... (946 chars)]`

### Example 3: Real-time Fraud Detection
High-priority inference for fraud detection:
`[code: -- Create fraud detection model with high priority ... (1169 chars)]`

### Example 4: Recommendation System
Product recommendations based on user behavior:
`[code: -- Register recommendation model ... (935 chars)]`

### Example 5: Named Entity Recognition (NER)
Extract entities from text documents:
`[code: -- Register NER model with compression for large documents ... (738 chars)]`

### Example 6: Stateful Sequence Model
Use stateful models (RNN/LSTM) with sequence tracking:
`[code: -- Register stateful conversation model ... (813 chars)]`

### Example 7: Batch Inference
Perform batch inference on historical data:
`[code: -- Register model for batch processing ... (918 chars)]`

### Example 8: Secured Triton Server
Access a secured Triton server with authentication:
`[code: -- Register model with authentication ... (462 chars)]`

Never hardcode sensitive tokens in SQL. Use Flink's secret management or environment variables.

### Example 9: Array Type with Flatten Batch Dimension
For models that accept array inputs without batch dimension:
`[code: -- Create model with array input and flatten batch dimension ... (712 chars)]`

### Example 10: Advanced Configuration
For production environments with comprehensive settings:
`[code: CREATE MODEL triton_advanced_model ... (464 chars)]`

## Model Options

### Required Options
[table: 5 rows]

### Optional Options
[table: 9 rows]

## Schema Requirement
[table: 5 rows]

Note: Input and output types must match the types defined in your Triton model configuration. To verify your model's expected input/output types, query the Triton server:
`[code: curl http://triton-server:8000/v2/models/{model_name}/config ... (60 chars)]`

## Triton Server Setup

To use this integration, you need a running Triton Inference Server. Here's a basic setup guide:

### Using Docker
`[code: # Pull Triton server image ... (311 chars)]`

### Model Repository Structure
Your model repository should follow this structure:
`[code: model_repository/ ... (209 chars)]`

### Example Model Configuration
Here's an example config.pbtxt for a text classification model:
`[code: name: "text-classification" ... (234 chars)]`

## Performance Considerations

- Connection Pooling: HTTP clients are pooled and reused for efficiency
- Asynchronous Processing: Non-blocking requests prevent thread starvation
- Batch Processing: Configure batch size for optimal throughput
  - Simple models: batch-size 1-4
  - Medium models: batch-size 4-16
  - Complex models: batch-size 16-32
- Resource Management: Automatic cleanup of HTTP resources
- Timeout Configuration: Set appropriate timeout values based on model complexity
  - Simple models: 1-5 seconds
  - Medium models (e.g., BERT): 5-30 seconds
  - Complex models (e.g., GPT): 30-120 seconds
- Retry Strategy: Configure retry attempts for handling transient failures
- Compression: Enable gzip compression for payloads > 1KB
- Parallelism: Match Flink parallelism to Triton server capacity

## Best Practices

### Model Version Management
Pin model versions in production to ensure consistency:
`[code: 'model-version' = '3'  -- Pin to version 3 instead of 'latest' ... (62 chars)]`

### Error Handling
Use default values on failure:
`[code: SELECT COALESCE(output, 'UNKNOWN') AS prediction ... (69 chars)]`

### Resource Configuration
Configure sufficient memory and network buffers for high-throughput scenarios:
`[code: taskmanager.memory.managed.size: 2gb ... (77 chars)]`

## Error Handling

The integration includes comprehensive error handling:
- Connection Errors: Automatic retry with exponential backoff
- Timeout Handling: Configurable request timeouts
- HTTP Errors: Detailed error messages from Triton server
- Serialization Errors: JSON parsing and validation errors

## Monitoring and Debugging

Enable debug logging to monitor the integration:
`[code: # In log4j2.properties ... (101 chars)]`

This will provide detailed logs about:
- HTTP request/response details
- Client connection management
- Error conditions and retries
- Performance metrics

## Troubleshooting

### Connection Issues
- Verify Triton server is running: curl http://triton-server:8000/v2/health/ready
- Check network connectivity and firewall rules
- Ensure endpoint URL includes the correct protocol (http/https)

### Timeout Errors
- Increase timeout value: 'timeout' = '60000'
- Check Triton server resource usage (CPU/GPU)
- Monitor Triton server logs for slow model execution

### Type Mismatch
- Verify model schema: curl http://triton-server:8000/v2/models/{model}/config
- Cast Flink types explicitly: CAST(value AS FLOAT)
- Ensure array dimensions match model expectations

### High Latency
- Enable request compression: 'compression' = 'gzip'
- Increase Triton server instances
- Use dynamic batching in Triton server configuration
- Check network latency between Flink and Triton

## Dependencies

To use the Triton model function, you need to include the following dependency in your Flink application:
`[code: <dependency> ... (154 chars)]`

## Further Information
- Triton Inference Server Documentation
- Triton Model Configuration
- Flink Async I/O
- Flink Metrics

---

Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/models/downloads/

# SQL Models download page

The page contains links to optional SQL Client models that are not part of the binary distribution.

# Optional SQL models
[table: 2 rows]