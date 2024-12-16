import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
import os
import subprocess
import yaml
from sample_apache_airflow.dags.dag.backup import _find_latest_base_backup, _get_backup_sets, _remove_backup_set

# Load config
dag_dir = os.path.dirname(os.path.realpath(__file__))
config_path = '../dags/dag/backup_config.yml'
with open(config_path) as f:
    config = yaml.safe_load(f)

class TestBackupFunctions(unittest.TestCase):

    @patch('os.listdir')
    def test_find_latest_base_backup(self, mock_listdir):
        mock_listdir.return_value = ['20240101_000000', '20240102_000000']
        latest_backup = _find_latest_base_backup('mybackup')
        self.assertIn('20240102_000000', latest_backup)

    @patch('os.listdir')
    def test_get_backup_sets(self, mock_listdir):
        mock_listdir.return_value = ['20240101_000000', '20240102_000000']
        backup_sets = _get_backup_sets('/backup/data/postgresql1/mydb/mybackup')
        self.assertEqual(len(backup_sets), 2)

    @patch('subprocess.run')
    def test_remove_backup_set(self, mock_run):
        _remove_backup_set('/backup/data/postgresql1/mydb/mybackup/20240101_000000')
        mock_run.assert_called_with(['rm', '-rf', '/backup/data/postgresql1/mydb/mybackup/20240101_000000'], check=True)

if __name__ == '__main__':
    unittest.main()