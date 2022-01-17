""" THIS IS THE FIRST VERSION OF THE PIPELINE BASED ON THE DYNAMIC APPROACH WHICH IS NOT FULLY COMPATIBLE WITH AIRFLOW
THE cannoli.py SCRIPT IS THE FINAL SOLUTION.
"""

from pathlib import Path

from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from bio_pipelines.helpers.cannoli_commands_args import get_aligner
from bio_pipelines.helpers.helpers import Samples
from bio_pipelines.operators.CannoliAlignmentOperator import CannoliAlignmentOperator
from bio_pipelines.operators.CannoliAnnotationOperator import CannoliAnnotationOperator
from bio_pipelines.operators.CannoliVariantCallingOperator import CannoliVariantCallingOperator
from bio_pipelines.operators.utils import interleave_fastq

args = {
    'owner': 'user',
    'start_date': days_ago(1),
}


def index_exists(exists, **kwargs):
    if exists == "True":
        return 'move_fromLocal_toHdfs'
    else:
        return ['index', 'move_fromLocal_toHdfs']


samples = Samples(Variable.get('NGS_workspace'))
aligner = get_aligner()

with DAG('BioDNASeq', default_args=args, schedule_interval='0 0 * * *', catchup=True) as dag:
    # In Cannoli some data should be placed in the same path locally and in hdfs
    move_to_hdfs = BashOperator(task_id="move_fromLocal_toHdfs",
                                bash_command="echo 5",
                                trigger_rule='none_failed')

    for sample_file in samples.get_all_files():
        sample = Path(sample_file).stem
        fastqc = BashOperator(task_id="fastqc_" + sample,
                              bash_command='docker run --rm -v {{var.value.get("NGS_workspace")}}:{{var.value.get("NGS_workspace")}} pegi3s/fastqc ' + sample_file)
    for id, sample in samples.get_paired_samples():
        fq1 = sample[0]
        fq2 = sample[1]
        interleaver = PythonOperator(
            task_id=id + '_fastq_interleaver',
            python_callable=interleave_fastq,
            op_kwargs={
                'paired_end1': fq1,
                'paired_end2': fq2,
                'samples': Samples('{{var.value.get("NGS_workspace")}}')
            },
            provide_context=True)
        interleaver >> move_to_hdfs
    for id, sample in samples.get_pipeline_samples():
        sample_path = " ".join(sample)
        index = BashOperator(task_id='index',
                             bash_command=' {{macros.get_index_command() }} ')
        should_index = BranchPythonOperator(task_id='should_index',
                                            python_callable=index_exists,
                                            op_kwargs={'exists': '{{macros.get_aligner().index_exists()}}'}
                                            )
        alignment = CannoliAlignmentOperator(
            task_id="Alignment_" + id,
            conn_id=Variable.get('bio_pipe_spark_connection'),
            java_class="BioPipeline",
            num_executors='{{var.value.get("bio_pipe_alignment_spark_num_executors", 1)}}',
            executor_memory='{{var.value.get("bio_pipe_alignment_spark_executor_memory", "2g")}}',
            driver_memory='{{var.value.get("bio_pipe_alignment_spark_driver_memory", "2g")}}',
            id=id,
            cannoli_path='{{var.value.get("bio_pipe_jar")}}',
            sample=sample_path,
            tool='{{var.value.get("bio_pipe_aligner")}}',
            location='{{var.value.get("NGS_workspace")}}',
            snps='{{var.value.get("bio_pipe_snps_file")}}',
            reference='{{var.value.get("bio_pipe_reference")}}')

        variant_calling_freebayes = CannoliVariantCallingOperator(
            task_id="VariantCalling_" + id,
            conn_id=Variable.get('bio_pipe_spark_connection'),
            num_executors='{{ var.value.get("bio_pipe_variantcalling_spark_num_executors", "1") }}',
            executor_memory='{{ var.value.get("bio_pipe_variantcalling_spark_executor_memory","2g" )}}',
            driver_memory='{{ var.value.get("bio_pipe_variantcalling_spark_driver_memory", "2g") }}',
            cannoli_path='{{ var.value.get("bio_pipe_cannoli") }}',
            tool="freebayes",
            id=id,
            location=Variable.get('NGS_workspace'),
            reference=Variable.get('bio_pipe_reference'))

        annotation_vep = CannoliAnnotationOperator(
            task_id="Annotation_" + id,
            conn_id='{{ var.value.get("bio_pipe_spark_connection", "1") }}',
            num_executors='{{ var.value.get("bio_pipe_annotation_spark_num_executors" , "1") }}',
            executor_memory='{{ var.value.get("bio_pipe_annotation_spark_executor_memory", "2g") }}',
            driver_memory='{{ var.value.get("bio_pipe_annotation_spark_driver_memory", "2g")}}',
            cannoli_path='{{ var.value.get("bio_pipe_cannoli")}}',
            id=id,
            location='{{ var.value.get("NGS_workspace") }}',
            tool='vep',
            cache='{{ var.value.get("bio_pipe_vep_cache") }}'
        )

        should_index >> move_to_hdfs >> alignment >> variant_calling_freebayes >> annotation_vep
        should_index >> index >> move_to_hdfs
