# Backup Postgresql and Mysql

This repository contains DAG recepie for backing up databases over network. The schedule is configured through the `backup_config.yaml` file. The DAG will automatically create full and incremental backups based on the schedule and retention period. Old backup sets are cleaned up based on the retention period and only full sets will be removed to make sure there is no dangling incremental backups.

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

```
backup_location/
  database_name/
    20240315_120000/
      base.backup
      incremental.backup
    20240316_120000/
      incremental.backup
    ...
```
