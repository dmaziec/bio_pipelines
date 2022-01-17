# export AIRFLOW__CORE__UNIT_TEST_MODE=true
import os
import time
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

import airflow
import pendulum
from airflow import DAG
from airflow import settings
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.models import DagBag
from airflow.models.taskinstance import TaskInstance
from airflow.operators.bio_pipeline import SparkTaskCollector
from bio_pipelines.operators.spark_config_operator import push_execution_date
from sqlalchemy import create_engine, text

source_path = Path(__file__).resolve()
source_dir = source_path.parent


class TestSparkCollector(unittest.TestCase):
    def setUp(self):
        source_path = Path(__file__).resolve()
        source_dir = source_path.parent

        self.dagbag = DagBag(str(source_dir.absolute()) + "/dags")

        self.test_db = str(source_dir.absolute()) + "/test.db"
        if not os.path.exists(self.test_db):
            open(self.test_db, 'w').close()

        self.db_id = 'test_db'
        self.sqlite = 'sqlite:///' + self.test_db
        from airflow import settings
        session = settings.Session()
        spark_host_id = 'spark_host_test'
        q_spark_host_id = session.query(Connection).filter(Connection.conn_id == spark_host_id).filter(
            Connection.conn_type == 'spark').filter(Connection.host == 'local[*]')
        exists_spark_hosts = session.query(q_spark_host_id.exists()).scalar()
        if not exists_spark_hosts:
            spark_host_conn = Connection(
                conn_id=spark_host_id,
                conn_type='spark',
                host='local[*]'
            )
            session.add(spark_host_conn)
            session.commit()

        host_db = '/' + self.test_db
        q_spark_db = session.query(Connection).filter(Connection.conn_id == self.db_id).filter(
            Connection.conn_type == 'sqlite').filter(Connection.host == host_db)
        exists_q_spark_db = session.query(q_spark_db.exists()).scalar()
        if not exists_q_spark_db:
            spark_db_conn = Connection(
                conn_id=self.db_id,
                conn_type='sqlite',
                host=host_db
            )
            session.add(spark_db_conn)
            session.commit()
        session.close()

    @mock.patch('airflow.contrib.hooks.spark_submit_hook.SparkSubmitHook.submit', return_value=True)
    def test_SparkSubmitHookSubmit(self, SparkHookSubmit):
        dag = self.dagbag.get_dag('ex')
        spark_collector_operator = dag.get_task('spark_app')
        execution_date = datetime.now()
        task = TaskInstance(task=spark_collector_operator, execution_date=execution_date)
        task.render_templates()
        context = task.get_template_context()
        spark_collector_operator.render_template_fields(context)

        # to avoid TypeError: Object of type 'Pendulum' is not JSON serializable error
        context['execution_date'] = str(context['execution_date'])
        spark_collector_operator.execute(context)
        assert isinstance(spark_collector_operator, airflow.contrib.operators.spark_submit_operator.SparkSubmitOperator)
        SparkHookSubmit.assert_called_once()

    def testMissingDbIdentifier(self):
        sample = '/Volumes/Samsung_T5/ngs/read.ifq'
        reference = '/Volumes/Samsung_T5/ngs/reference/genome.fa'
        cannoli = "/Users/dominika/projects/cannoli-assembly-spark3_2.12-0.11.0-SNAPSHOT.jar"
        dag = DAG(dag_id='collector', start_date=datetime.now())
        with self.assertRaises(Exception):
            SparkTaskCollector(task_id='spark_app', executor_cores=4,
                               conf={"spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                                     "spark.kryo.registrator": "org.bdgenomics.adam.serialization.ADAMKryoRegistrator",
                                     "spark.yarn.driver.memoryOverhead": "2098",
                                     "spark.dynamicAllocation.enabled": "false"},
                               conn_id='cdh00',
                               java_class="org.bdgenomics.cannoli.cli.Cannoli",
                               num_executors=1,
                               executor_memory="2g",
                               driver_memory="2g",
                               application=cannoli,
                               application_args=['bwaMem', sample,
                                                 '/Volumes/Samsung_T5/sample_aligned.sam', '-index',
                                                 reference, '-sample_id', 'sample', '-use_docker',
                                                 '-single'],
                               dag=dag)

    # @mock.patch('airflow.models.taskinstance.TaskInstance.xcom_pull')
    def testXcomPullIfPossible(self):
        dag = self.dagbag.get_dag('ex')
        spark_collector_operator = dag.get_task('spark_app')
        execution_date = datetime.now()
        task = TaskInstance(task=spark_collector_operator, execution_date=execution_date)
        task.render_templates()
        context = task.get_template_context()
        spark_collector_operator.render_template_fields(context)

        spark_collector_operator.execute(context)

        collector_operator = dag.get_task('spark_appCollector')
        task = TaskInstance(task=collector_operator, execution_date=execution_date)
        execution_date_value = task.xcom_pull(task_ids='spark_app', key='spark_appexecution_date')
        execution_date_value_expected = pendulum.instance(execution_date)
        self.assertEqual(execution_date_value, execution_date_value_expected)

    @mock.patch('airflow.models.taskinstance.TaskInstance.xcom_pull')
    def testXcomPullIfCorrectParameters(self, xcom_pull):
        dag = self.dagbag.get_dag('ex')
        spark_collector_operator = dag.get_task('spark_app')
        execution_date = datetime.now()
        task = TaskInstance(task=spark_collector_operator, execution_date=execution_date)
        task.render_templates()
        context = task.get_template_context()
        spark_collector_operator.render_template_fields(context)

        xcom_pull.return_value = pendulum.instance(execution_date)

        start_time = time.time()
        spark_collector_operator.execute(context)
        end_time = time.time()
        duration = end_time - start_time

        session = settings.Session()
        task.state = 'success'
        task.duration = duration
        session.add(task)
        session.commit()

        collector_operator = dag.get_task('spark_appCollector')
        task = TaskInstance(task=collector_operator, execution_date=execution_date)
        task.render_templates()
        context = task.get_template_context()
        collector_operator.render_template_fields(context)
        collector_operator.execute(context)
        xcom_pull.assert_called_once_with(task_ids='spark_app', key='spark_appexecution_date')

    def testSuccessfulExecution(self):
        dag = self.dagbag.get_dag('ex')
        spark_collector_operator = dag.get_task('spark_app')
        execution_date = datetime.now()
        task = TaskInstance(task=spark_collector_operator, execution_date=execution_date)
        task.render_templates()
        context = task.get_template_context()
        spark_collector_operator.render_template_fields(context)

        start_time = time.time()
        spark_collector_operator.execute(context)
        end_time = time.time()
        duration = end_time - start_time

        session = settings.Session()
        task.state = 'success'
        task.duration = duration

        session.add(task)
        session.commit()

        collector_operator = dag.get_task('spark_appCollector')
        task = TaskInstance(task=collector_operator, execution_date=execution_date)
        execution_date_value = task.xcom_pull(task_ids='spark_app', key='spark_appexecution_date')
        task.render_templates()
        context = task.get_template_context()
        collector_operator.render_template_fields(context)

        collector_operator.execute(context)

        query = text(
            "SELECT * FROM spark_collector WHERE task_id=:task_id AND dag_id = :dag_id AND execution_date=:execution_date")
        engine = create_engine(self.sqlite)
        conn = engine.connect()
        data = conn.execute(query,
                            {'task_id': 'spark_app', 'dag_id': 'ex', 'execution_date': execution_date}).fetchall()

        self.assertEqual(1, len(data))
        row = data[0]

        self.assertEqual((row['task_id']), 'spark_app')
        self.assertEqual((row['dag_id']), 'ex')
        self.assertEqual((row['execution_date']), execution_date.strftime("%Y-%m-%d %H:%M:%S.%f"))
        self.assertEqual((row['executor_memory']), spark_collector_operator._executor_memory)
        self.assertEqual((row['host']), 'local[*]')
        self.assertEqual((row['executor_cores']), 4)
        self.assertEqual((row['driver_memory']), spark_collector_operator._driver_memory)
        self.assertEqual(row['application_name'], 'airflow-spark')
        self.assertEqual(row['state'], 'success')
        self.assertEqual(row['duration'], duration)

    @mock.patch('bio_pipelines.operators.spark_config_operator.push_execution_date', return_value=True)
    def testFailedExecution(self, failure_func):
        dag = self.dagbag.get_dag('ex')
        spark_collector_operator = dag.get_task('spark_app')
        execution_date_sc = datetime.now()
        task = TaskInstance(task=spark_collector_operator, execution_date=execution_date_sc)
        task.render_templates()
        context = task.get_template_context()
        spark_collector_operator.render_template_fields(context)
        task.handle_failure(AirflowException, force_fail=True, context=context)

        failure_func.assert_called_once()
        failure_func.assert_called_with(context)

    @mock.patch('airflow.models.taskinstance.TaskInstance.xcom_push', return_value=True)
    def test_push_execution_date(self, xcom_push):
        dag = self.dagbag.get_dag('ex')
        spark_collector_operator = dag.get_task('spark_app')
        execution_date_sc = datetime.now()
        task = TaskInstance(task=spark_collector_operator, execution_date=execution_date_sc)
        task.render_templates()
        context = task.get_template_context()
        push_execution_date(context)
        task_id = context['task_instance'].task_id + 'execution_date'
        execution_date = context['execution_date']
        xcom_push.assert_called_once()
        xcom_push.assert_called_with(task_id, execution_date)
