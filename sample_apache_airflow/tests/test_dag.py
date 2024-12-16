import unittest
from airflow.models import DagBag

class TestDagIntegrity(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag()

    def test_dag_loaded(self):
        dag = self.dagbag.get_dag(dag_id='database_backup_mybackup')
        self.assertIsNotNone(dag)
        self.assertListEqual(
            sorted([task.task_id for task in dag.tasks]),
            ['base_backup', 'cleanup_old_backups', 'incremental_backup']
        )

if __name__ == '__main__':
    unittest.main()
