# Setting up ETL Pipeline to pull API data daily using Airflow

Prerequisites:
- Docker and docker compose are installed

### 1. Clone latest docker-airflow repo into folder

`git clone https://github.com/puckel/docker-airflow.git' `

This will download a docker-airflow setup folder into your current working directory.


### 2. Pull image from docker repository

`docker pull puckel/docker-airflow`


### 3. Build , adding extra dependencies if necessary

`docker build --rm --build-arg AIRFLOW_DEPS="gcp, dask, virtualenv" -t puckel/docker-airflow .`

**Note** Make sure you are in the /docker-airflow directory when running above. Run ls command and you should see a docker file, otherwise you will get build error.

### 4. Create docker-compose file (using LocalExecutor here)

`docker-compose -f docker-compose-LocalExecutor.yml up -d`

- LocalExecutor = exemplifies single-node architecture and completes tasks in parallel that run on a single machine — ideal for testing.
- CeleryExecutor = focus on horizontal scaling and run python processes in a distributed style with a pool of independent workers — the best choice for users in production and ideal for massive amounts of jobs but take longer to set up.
- SequentialExecutor = default, only executor that can be used with sqlite since sqlite doesn't support multiple connections. This executor will only run one task instance at a time. For production use case, please use other executors.