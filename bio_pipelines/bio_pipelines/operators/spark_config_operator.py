from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Connection, TaskInstance
from airflow.operators.python_operator import PythonOperator
from airflow.utils.db import provide_session
from airflow.utils.log.logging_mixin import LoggingMixin
from sqlalchemy import Column, String, Float, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

"""
SparkTaskCollector is SparkSubmitOperator with ability to store application configuration with time duration of a task  
How to run collector: 
1. Declare: 
spark_application = SparkTaskCollector(dag_id = 'spark_app', executor_cores = 4 , etc .. 
collector = spark_application.run_collector

2. Set dependencies in correct order:
spark_application >> collector
"""
Base = declarative_base()


class SparkCollector(Base):
    __tablename__ = 'spark_collector'
    task_id = Column('task_id', String, primary_key=True)
    dag_id = Column('dag_id', String, primary_key=True)
    execution_date = Column('execution_date', DateTime, primary_key=True)
    executor_memory = Column('executor_memory', String, nullable=True)
    driver_memory = Column('driver_memory', String, nullable=True)
    executor_cores = Column('executor_cores', Integer, nullable=True)
    host = Column('host', String)
    app_name = Column('application_name', String)
    duration = Column('duration', Float)
    state = Column('state', String)

    # optional configuration from --conf flag
    # executor_memoryOverhead = Column('executor_memoryOverhead', Integer, nullable=True)
    # hadoop_mapreduce_input_fileinputformat_split_maxsize = Column(
    #    'hadoop_mapreduce_input_fileinputformat_split_maxsize', Integer, nullable=True)


def push_execution_date(context):
    task_instance = context['task_instance']
    task_id = context['task_instance'].task_id
    task_instance.xcom_push(task_id + 'execution_date', context['execution_date'])


def failure(context):
    push_execution_date(context)


class SparkTaskCollector(SparkSubmitOperator, LoggingMixin):
    ui_color = "#DFB9BD"

    def __init__(self, collector_id=None,
                 **kwargs):
        super().__init__(on_failure_callback=failure, **kwargs)  # on_failure_callback=failure
        if collector_id is None:
            raise Exception("Cannot run spark collector without specified database connection - collector_id")
        self.collector_id = collector_id

        self.run_collector = PythonOperator(task_id=self.task_id + "Collector",
                                            python_callable=self._run,
                                            op_kwargs={'task_id': self.task_id,  # key
                                                       'dag_id': self.dag.dag_id,  # key
                                                       'conn_id_spark': self._conn_id,
                                                       'conn_id_db': self.collector_id,
                                                       'executor_cores': self._executor_cores,
                                                       'executor_memory': self._executor_memory,
                                                       'application_name': self._name,
                                                       'driver_memory': self._driver_memory,
                                                       'conf': self._conf},
                                            dag=self.dag,
                                            provide_context=True,
                                            trigger_rule='all_done')

    # provide_session provides a connection with Airflow database
    @provide_session
    def _run(self, session=None, **kwargs):

        task_id = kwargs['task_id']
        dag_id = kwargs['dag_id']
        task_instance = kwargs['task_instance']
        connection_spark_id = kwargs.get('conn_id_spark')
        application_name = kwargs['application_name']

        # when conn is not passed, yarn is set by default
        if connection_spark_id == 'spark_default':
            connection_spark_id = 'yarn'

        execution_date = task_instance.xcom_pull(task_ids=task_id, key=task_id + 'execution_date')
        task_instance_row = session.query(TaskInstance.duration).filter(TaskInstance.task_id == task_id).filter(
            TaskInstance.dag_id == dag_id).filter(TaskInstance.execution_date == execution_date).one()
        duration = task_instance_row.duration

        task_instance_row = session.query(TaskInstance.state).filter(TaskInstance.task_id == task_id).filter(
            TaskInstance.dag_id == dag_id).filter(TaskInstance.execution_date == execution_date).one()
        task_state = task_instance_row.state
        self.log.info("Collected duration of task {0}".format(duration))

        connection_spark = session.query(Connection.host).filter(Connection.conn_id == connection_spark_id).one()
        host = connection_spark.host

        db_hook = session.query(Connection).filter(Connection.conn_id == kwargs['conn_id_db']).one().get_hook()
        collector_engine = db_hook.get_sqlalchemy_engine()
        Base.metadata.create_all(bind=collector_engine)
        Session = sessionmaker()
        Session.configure(bind=collector_engine)
        session_ = Session()

        row = SparkCollector(task_id=kwargs['task_id'],
                             dag_id=kwargs['dag_id'],
                             execution_date=execution_date,
                             executor_memory=kwargs['executor_memory'],
                             duration=duration,
                             host=host,
                             app_name=application_name,
                             state=task_state,
                             executor_cores=kwargs['executor_cores'],
                             driver_memory=kwargs['driver_memory'])
        # hadoop_mapreduce_input_fileinputformat_split_maxsize=
        # kwargs['conf'].get('spark.hadoop.mapreduce.input.fileinputformat.split.maxsize'),
        # executor_memoryOverhead=kwargs['conf'].get('spark.executor.memoryOverhead'))
        session_.add(row)
        session_.commit()
        self.log.info("Successfully added collected spark execution data")

    def execute(self, context):
        super().execute(context)
        push_execution_date(context)
        self.log.info("Successful execute")
