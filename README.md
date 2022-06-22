# spark-streaming-iceberg"

## Problem Definition

![events filtered](https://github.com/spraja08/event-storming/blob/main/images/2-events.png)

## Usage Instructions

1) Read CSV Data From File System (S3) using DataStreamReader (Spark Structured Streaming)
2) Perform Selection of Specific Columns
3) Perform Windowed Aggreation
4) Write into Iceberg Tables (on S3) using Glue Catalog