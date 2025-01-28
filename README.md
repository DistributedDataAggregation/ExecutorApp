# Distributed Data Aggregation System - ExecutorApp
The executor is a compute node within the aggregation system. It receives tasks to calculate aggregates on tables stored in Parquet files. The executor processes these tasks, then either sends the results to the main executor in the system or gathers intermediate results from other executors, combines them, and sends the final output back to the controller.

For public API, please refer to repository of controller module: https://github.com/DistributedDataAggregation/ControllerApp

## How to run
The recommended way of running the executor app is through Docker. Just run the script file to build docker image

### 1. Specify enviroment variables
In .env file you'll need to specify two variables
```
EXECUTOR_CONTROLLER_PORT=8080
EXECUTOR_EXECUTOR_PORT=8081
```

### 2. Build docker image

```bash
./build-docker-image.sh
```
### 3. Run the container 
```bash
./run_docker.sh
```

## Build locally
For local builds, please refer to Dockerfile.
Each instruction matches exactly the process of installing on clear ubuntu setup.
