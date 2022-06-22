# spark-streaming-iceberg"

## Problem Definition

![events filtered](https://github.com/spraja08/spark-streaming-iceberg/blob/main/images/functional-overview.png)

## Usage Instructions

1) Navigage to the app directory in the terminal
2) Customise the bucket name (line 14) in the Makefile and the EMR Cluster id (line 16)
3) Run 'make build' (docker needs to be installed and the docker daemon needs to be running) 
4) The above command will creat the dist directory and package all the deployment units.
5) Navigate to the dist directory
6) Execute the run.sh script (Dependencies : aws cli, an EMR cluster pre-configured and running)