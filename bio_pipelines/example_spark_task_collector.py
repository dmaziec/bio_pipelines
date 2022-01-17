
from airflow.models import DAG
from airflow.operators.bio_pipeline import SparkTaskCollector
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

sample = '/Volumes/Samsung_T5/ngs/read.ifq'
reference = '/Volumes/Samsung_T5/ngs/reference/genome.fa'

cannoli = "/Users/dominika/projects/cannoli-assembly-spark3_2.12-0.11.0-SNAPSHOT.jar"

with DAG('ex', default_args=args, schedule_interval='0 0 * * *', catchup=True) as dag:
    spark_application = SparkTaskCollector(task_id='spark_app', executor_cores=4,
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
                                           application_args=['bwaMem', sample, '/Volumes/Samsung_T5/sample_aligned.sam',
                                                             '-index', reference, '-sample_id', 'sample', '-use_docker',
                                                             '-single'],
                                           collector_id="sparkCollector"
                                           )

    collector = spark_application.run_collector

    spark_application >> collector
