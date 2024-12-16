from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import yaml
import os
import subprocess
from typing import Dict, List
import logging

# Load config
with open('backup_config.yaml') as f:
    config = yaml.safe_load(f)

class BackupHandler:
    @staticmethod
    def get_handler(db_type: str):
        if db_type.lower() == 'postgresql':
            return PostgresBackupHandler()
        elif db_type.lower() == 'mysql':
            return MySQLBackupHandler()
        raise ValueError(f"Unsupported database type: {db_type}")

class PostgresBackupHandler:
    def base_backup_cmd(self, db_config: Dict, backup_path: str) -> List[str]:
        return [
            'pg_basebackup',
            '-h', db_config['host'],
            '-p', str(db_config['port']),
            '-U', db_config['user'],
            '-D', backup_path,
            '-Ft',
            '-z',
            '-P'
        ]
    
    def incremental_backup_cmd(self, db_config: Dict, backup_path: str, base_path: str) -> List[str]:
        return [
            'pg_dump',
            '-h', db_config['host'],
            '-p', str(db_config['port']),
            '-U', db_config['user'],
            '-Fc',
            '-Z9',
            db_config['database'],
            '-f', backup_path
        ]

class MySQLBackupHandler:
    def base_backup_cmd(self, db_config: Dict, backup_path: str) -> List[str]:
        return [
            'mysqldump',
            '-h', db_config['host'],
            '-P', str(db_config['port']),
            '-u', db_config['user'],
            f"--password={db_config['password']}",
            '--all-databases',
            '--single-transaction',
            f'> {backup_path}'
        ]
    
    def incremental_backup_cmd(self, db_config: Dict, backup_path: str, base_path: str) -> List[str]:
        return [
            'mysqlbinlog',
            '--start-datetime', f"{self._get_last_backup_time(base_path)}",
            '--stop-datetime', 'now',
            f'> {backup_path}'
        ]

def perform_backup(db_name: str, backup_type: str, **context):
    db_config = config['databases'][db_name]
    backup_settings = config['backup_settings']
    
    handler = BackupHandler.get_handler(db_config['type'])
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_dir = os.path.join(
        backup_settings['backup_location'],
        db_name,
        timestamp
    )
    os.makedirs(backup_dir, exist_ok=True)
    
    backup_path = os.path.join(backup_dir, f"{backup_type}.backup")
    
    if backup_type == 'base':
        cmd = handler.base_backup_cmd(db_config, backup_path)
    else:
        base_backup = _find_latest_base_backup(db_name)
        cmd = handler.incremental_backup_cmd(db_config, backup_path, base_backup)
    
    env = os.environ.copy()
    env['PGPASSWORD'] = db_config['password']  # For PostgreSQL
    
    result = subprocess.run(cmd, env=env, check=True)
    if result.returncode != 0:
        raise Exception(f"Backup failed with exit code {result.returncode}")

def cleanup_old_backups(db_name: str, **context):
    backup_settings = config['backup_settings']
    backup_root = os.path.join(backup_settings['backup_location'], db_name)
    retention_date = datetime.now() - timedelta(days=backup_settings['retention_days'])
    
    # Find backup sets older than retention period
    for backup_set in _get_backup_sets(backup_root):
        set_date = datetime.strptime(backup_set.split('_')[0], '%Y%m%d')
        if set_date < retention_date:
            logging.info(f"Removing old backup set: {backup_set}")
            _remove_backup_set(os.path.join(backup_root, backup_set))

# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

for db_name, db_config in config['databases'].items():
    dag_id = f'database_backup_{db_name}'
    
    with DAG(
        dag_id,
        default_args=default_args,
        description=f'Database backup DAG for {db_name}',
        schedule_interval=config['backup_settings']['incremental_schedule'],
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=['backup'],
    ) as dag:
        
        # Base backup task
        base_backup = PythonOperator(
            task_id='base_backup',
            python_callable=perform_backup,
            op_kwargs={'db_name': db_name, 'backup_type': 'base'},
            trigger_rule='all_success'
        )
        
        # Incremental backup task
        incremental_backup = PythonOperator(
            task_id='incremental_backup',
            python_callable=perform_backup,
            op_kwargs={'db_name': db_name, 'backup_type': 'incremental'},
            trigger_rule='all_success'
        )
        
        # Cleanup task
        cleanup = PythonOperator(
            task_id='cleanup_old_backups',
            python_callable=cleanup_old_backups,
            op_kwargs={'db_name': db_name},
            trigger_rule='all_success'
        )
        
        # Set dependencies
        base_backup >> incremental_backup >> cleanup