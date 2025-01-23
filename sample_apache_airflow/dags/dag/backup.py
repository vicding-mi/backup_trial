from time import timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import yaml
import os
import subprocess
from typing import Dict, List
import logging
import requests
import pendulum

logger = logging.getLogger(__name__)


def load_config():
    try:
        response = requests.get(config_url)
        response.raise_for_status()
        config = yaml.safe_load(response.text)
        logger.info("Config loaded from internet.")
    except (requests.RequestException, yaml.YAMLError):
        with open(config_path) as f:
            config = yaml.safe_load(f)
            logger.info("Config loaded from local file.")
    return config


# Load config
env = os.environ.copy()
dag_dir = os.path.dirname(os.path.realpath(__file__))
config_path = os.path.join(dag_dir, 'backup_config.yml')
config_url = env.get('BACKUP_CONFIG_URL', "")

config = load_config()


### MessageProvider
class MessageProvider:
    def send_message(self, message: str, webhook_url: str):
        raise NotImplementedError("This method should be overridden by subclasses")


class SlackMessageProvider(MessageProvider):
    def send_message(self, message: str, webhook_url: str):
        logger.error(f"Sending message to Slack: {message} using webhook: {webhook_url}")
        response = requests.post(webhook_url, json={'text': message})
        response.raise_for_status()

        # slack_message = SlackWebhookOperator(
        #     task_id='send_slack_message',
        #     slack_webhook_conn_id='slack_webhook',
        #     webhook_token=webhook_url,
        #     message=message,
        #     username='airflow'
        # )
        # slack_message.execute(context={})


def get_message_provider(provider_name: str) -> MessageProvider:
    if provider_name == 'slack':
        return SlackMessageProvider()
    # Add more providers here as needed
    else:
        raise ValueError(f"Unsupported message provider: {provider_name}")


def notify_on_success(context):
    task_instance = context['task_instance']
    backup_path = task_instance.xcom_pull(task_ids='base_backup_task', key='backup_path')
    backup_size = os.path.getsize(backup_path)
    start_time = task_instance.start_date
    end_time = task_instance.end_date
    message = (
        f":white_check_mark: Task {task_instance.task_id} succeeded in DAG {task_instance.dag_id}\n"
        f"Start time: {start_time}\n"
        f"End time: {end_time}\n"
        f"Backup file location: {backup_path}\n"
        f"Backup file size: {backup_size} bytes"
    )
    provider = get_message_provider(context["db_config"]["notification_provider"])
    provider.send_message(message, context["db_config"]['notification_url'])


def notify_on_failure(context):
    task_instance = context['task_instance']
    backup_path = task_instance.xcom_pull(task_ids='base_backup_task', key='backup_path')
    if os.path.isfile(backup_path):
        backup_size = os.path.getsize(backup_path)
    else:
        backup_size = -1
    start_time = task_instance.start_date
    end_time = task_instance.end_date

    message = (
        f":x: Task {task_instance.task_id} failed in DAG {task_instance.dag_id}\n"
        f"Start time: {start_time}\n"
        f"End time: {end_time}\n"
        f"Backup file location: {backup_path}\n"
        f"Backup file size: {backup_size} bytes"
    )
    provider = get_message_provider(context["db_config"]["notification_provider"])
    provider.send_message(message, context["db_config"]['notification_url'])


### BackupHandler
class BackupHandler:
    @staticmethod
    def get_handler(db_type: str):
        if db_type.lower() == 'postgresql':
            return PostgresBackupHandler()
        elif db_type.lower() == 'mysql':
            return MySQLBackupHandler()
        raise ValueError(f"Unsupported database type: {db_type}")


class PostgresBackupHandler:
    # def base_backup_cmd(self, db_config: Dict, backup_path: str) -> List[str]:
    #     return [
    #         'pg_basebackup',
    #         '-h', db_config['host'],
    #         '-p', str(db_config['port']),
    #         '-U', db_config['user'],
    #         '-D', backup_path,
    #         '-Ft',
    #         '-z',
    #         '-P'
    #     ]
    def base_backup_cmd(self, db_config: Dict, backup_path: str) -> List[str]:
        return [
            'pg_dump',
            '-h', db_config['host'],
            '-p', str(db_config['port']),
            '-U', db_config['user'],
            '-Fp',
            db_config['database'],
            '-f', backup_path
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
            f'{db_name}',
            # '--single-transaction',
            # f'> {backup_path}'
        ]

    def incremental_backup_cmd(self, db_config: Dict, backup_path: str, base_path: str) -> List[str]:
        return [
            'mysqlbinlog',
            '--start-datetime', f"{self._get_last_backup_time(base_path)}",
            '--stop-datetime', 'now',
            f'> {backup_path}'
        ]


def _find_latest_base_backup(db_name: str) -> str:
    backup_settings = config['backup_settings']
    backup_root = os.path.join(backup_settings['backup_location'], db_name)
    base_backups = [d for d in os.listdir(backup_root) if os.path.isdir(os.path.join(backup_root, d))]
    base_backups.sort(reverse=True)
    if not base_backups:
        raise Exception(f"No base backups found for database {db_name}")
    return os.path.join(backup_root, base_backups[0])


def _get_backup_sets(backup_root: str) -> List[str]:
    return [d for d in os.listdir(backup_root) if os.path.isdir(os.path.join(backup_root, d))]


def _remove_backup_set(backup_set_path: str):
    subprocess.run(['rm', '-rf', backup_set_path], check=True)


def cleanup_old_backups(db_name: str, **context):
    backup_settings = config['backup_settings']
    backup_root = os.path.join(backup_settings['backup_location'], db_name)
    retention_date = datetime.now() - timedelta(days=backup_settings['retention_days'])

    # Find backup sets older than retention period
    for backup_set in _get_backup_sets(backup_root):
        set_date = datetime.strptime(backup_set.split('_')[0], '%Y%m%d')
        if set_date < retention_date:
            logger.info(f"Removing old backup set: {backup_set}")
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

# Define the timezone
local_tz = pendulum.timezone(env.get("TZ", "Europe/Amsterdam"))


# Updated perform_backup function
def perform_backup(db_name: str, **context):
    db_config = config['databases'][db_name]
    backup_settings = config['backup_settings']

    # Read backup type from config, default to 'base'
    backup_type = db_config.get('backup_type', 'base')
    handler = BackupHandler.get_handler(db_config['type'])

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_dir = os.path.join(
        backup_settings['backup_location'],
        db_name,
        timestamp
    )
    os.makedirs(backup_dir, exist_ok=True)

    backup_path = os.path.join(backup_dir, f"{backup_type}.backup")
    context['task_instance'].xcom_push(key='backup_path', value=backup_path)

    if backup_type == 'base':
        cmd = handler.base_backup_cmd(db_config, backup_path)
    else:
        base_backup = _find_latest_base_backup(db_name)
        cmd = handler.incremental_backup_cmd(db_config, backup_path, base_backup)

    env['PGPASSWORD'] = db_config['password']

    if db_config['type'] == "mysql":
        with open(backup_path, 'w') as f:
            result = subprocess.run(cmd, env=env, check=True, stdout=f)
    else:
        result = subprocess.run(cmd, env=env, check=True)
    if result.returncode != 0:
        raise Exception(f"Backup failed with exit code {result.returncode} {result.stdout}")


# Updated DAG definition
for db_name, db_config in config['databases'].items():
    backup_type = db_config.get('backup_type', 'base')

    if backup_type == 'base':
        dag_id = f'database_base_backup_{db_name}'

        with DAG(
                dag_id,
                default_args=default_args,
                description=f'Base backup DAG for {db_name}',
                schedule_interval=config['backup_settings']['base_backup_schedule'],
                start_date=datetime(2025, 1, 1, tzinfo=local_tz),
                catchup=False,
                tags=db_config['tags'],
        ) as dag:
            base_backup_task = PythonOperator(
                task_id='base_backup_task',
                python_callable=perform_backup,
                op_kwargs={'db_name': db_name, 'backup_type': backup_type, 'db_config': db_config},
                trigger_rule='all_success',
                on_success_callback=notify_on_success if db_config['notification_type'] == 'all' else None,
                on_failure_callback=notify_on_failure
            )

            cleanup_base = PythonOperator(
                task_id='cleanup_old_backups',
                python_callable=cleanup_old_backups,
                op_kwargs={'db_name': db_name, 'backup_type': backup_type, 'db_config': db_config},
                trigger_rule='all_success',
                # on_success_callback=notify_on_success if db_config['notification_type'] == 'all' else None,
                on_failure_callback=notify_on_failure
            )

            base_backup_task >> cleanup_base

    # elif backup_type == 'incremental':
    #     dag_id = f'database_incremental_backup_{db_name}'
    #
    #     with DAG(
    #             dag_id,
    #             default_args=default_args,
    #             description=f'Incremental backup DAG for {db_name}',
    #             schedule_interval=config['backup_settings']['incremental_schedule'],
    #             start_date=datetime(2024, 1, 1),
    #             catchup=False,
    #             tags=db_config['tags'],
    #     ) as dag:
    #         base_backup_task = PythonOperator(
    #             task_id='base_backup_task',
    #             python_callable=perform_backup,
    #             op_kwargs={'db_name': db_name},
    #             trigger_rule='all_success'
    #         )
    #
    #         incremental_backup_task = PythonOperator(
    #             task_id='incremental_backup_task',
    #             python_callable=perform_backup,
    #             op_kwargs={'db_name': db_name},
    #             trigger_rule='all_success'
    #         )
    #
    #         cleanup_incremental = PythonOperator(
    #             task_id='cleanup_old_backups',
    #             python_callable=cleanup_old_backups,
    #             op_kwargs={'db_name': db_name},
    #             trigger_rule='all_success'
    #         )
    #
    #         base_backup_task >> incremental_backup_task >> cleanup_incremental
