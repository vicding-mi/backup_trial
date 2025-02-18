# Backup Postgresql and Mysql

This repository contains DAG recipe for backing up databases over network. The schedule is configured by using the `backup_config.yaml` file. The DAG will automatically create full and incremental backups based on the schedule and retention period. Old backup sets are cleaned up based on the retention period and only full sets will be removed to make sure there is no dangling incremental backups.
## TODO

- [x] Send message to Slack
- [ ] Test query for health check of graph db 
  - [ ] Web UI: https://data.goldenagents.org/query/https%3A%2F%2Fsparql2.goldenagents.org%2Frijksmuseum
  - [ ] Test script and query in tests folder `query.*`


## Key features of this implementation

Supports both PostgreSQL and MySQL backups
Configurable backup schedules and retention period
Handles both full and incremental backups
Cleanup of old backup sets
Environment variables for sensitive credentials
Error handling and logging
Parallel backup support through config
Extensible for other database types

## HowTo use

Place both files in your Airflow DAGs folder
Update the backup_config.yaml with your database settings
Set up database connection credentials in Airflow connections
The DAG will automatically schedule backups based on the configuration
The DAG creates a directory structure like:

```text
backup_location/
  database_name/
    20240315_120000/
      base.backup
      incremental.backup
    20240316_120000/
      incremental.backup
    ...
```

### Generate a new Fernet key

```python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"```

### Create Airflow Admin account

```shell
docker compose run airflow airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```
