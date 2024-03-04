# Code Challenge Repo

Data Integration Challenge Repo

## Table of Contents

- [Docker Compose](#docker-compose)
- [Examples](#examples)

## Docker Compose

For development purposes, a docker-compose file is included in the project (`/develop/docker-compose.yaml`) to create a PostgreSQL connection that can be accessed through a database tool, which DBeaver is the tool that is being used in this project.

PostgreSQL download link = https://www.postgresql.org/download/

Docker Desktop download link = https://www.docker.com/products/docker-desktop/

DBeaver download link = https://dbeaver.io/download/

How to run:

```
cd develop
docker-compose up -d
```

After that, open up your DBeaver then create a new PostgreSQL connection, where the connection details needed can be found on /dags/util/db/conn.py. You may try to test the connection after to make sure that you can use the connection.

## Examples

How to run the python script:

1. `cd dags`
2. format = `python3 {dag_file} {args}`. For the args, it depends on the dag file.
3. example = `python3 dag_salary_per_hour -d "2019-12-01" -ef "/Users/name/Downloads/employees.csv" -tf "/Users/name/Downloads/timesheets/csv"`

How to run the sql script:

1. Find the query on `./sql` folder.
2. Copy all of the content in the selected sql file (`E.G. hourly_salary.sql`)
3. Paste the content on your DBeaver SQL Editor where you should open the Editor based on this project database.
4. Block all the query (`CTRL+A` / `CMD+A`) then run the query (or you can use this shortcut -> `CTRL+Enter`)
