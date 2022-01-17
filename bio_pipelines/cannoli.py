from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.bio_pipeline import CannoliAlignmentOperator, CannoliAnnotationOperator, \
    CannoliVariantCallingOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'user',
    'start_date': days_ago(1),
}


def index_exists(exists, **kwargs):
    if exists == "True":
        return 'move_fromLocal_toHdfs'
    else:
        return ['index', 'move_fromLocal_toHdfs']


with DAG('BioDNASeq', default_args=args, schedule_interval='0 0 * * *', catchup=True) as dag:
    # In Cannoli some data should be placed in the same path locally and in hdfs
    move_to_hdfs = BashOperator(task_id="move_fromLocal_toHdfs",
                                bash_command='hdfs dfs -mkdir -p {{var.value.get("NGS_workspace")}} && hdfs dfs -put -f ' + '{{var.value.get("NGS_workspace")}}' + "/* " + '{{var.value.get("NGS_workspace")}}',
                                trigger_rule='none_failed')

    fastqc = BashOperator(task_id="Quality_Control",
                          bash_command='docker run --rm -v {{var.value.get("NGS_workspace")}}:{{var.value.get("NGS_workspace")}} pegi3s/fastqc -t {{var.value.get("bio_pipe_fastqc_threads", 1)}} {{var.value.get("bio_pipe_fastq")}}')

    index = BashOperator(task_id='index',
                         bash_command=' {{macros.get_index_command() }} ')
    should_index = BranchPythonOperator(task_id='should_index',
                                        python_callable=index_exists,
                                        op_kwargs={'exists': '{{macros.get_aligner().index_exists()}}'})
    alignment = CannoliAlignmentOperator(
        task_id="Alignment",
        conn_id='{{ var.value.get("bio_pipe_spark_connection") }}',
        java_class="BioPipeline",
        num_executors='{{var.value.get("bio_pipe_alignment_spark_num_executors", 1)}}',
        executor_memory='{{var.value.get("bio_pipe_alignment_spark_executor_memory", "2g")}}',
        driver_memory='{{var.value.get("bio_pipe_alignment_spark_driver_memory", "2g")}}',
        id='{{var.value.get("bio_pipe_prefix")}}',
        cannoli_path='{{var.value.get("bio_pipe_jar")}}',
        sample='{{var.value.get("bio_pipe_fastq")}}',
        tool='{{var.value.get("bio_pipe_aligner", "bowtie2")}}',
        output='{{var.value.get("NGS_workspace")}}' + '/{{var.value.get("bio_pipe_prefix")}}_aligned.sam',
        snps='{{var.value.get("bio_pipe_snps_file")}}',
        reference='{{var.value.get("bio_pipe_reference")}}',
        additional_args='{{var.value.get("bio_pipe_alignment_additional_args", None)}}',
        tool_args='{{var.value.get("bio_pipe_alignment_tool_args", None)}}',
        spark_binary='{{var.value.get("spark_binary", "spark-submit")}}',
        sequence_dictionary='{{var.value.get("bio_pipe_sequence_dict", "None")}}',
        image='{{var.value.get("bio_pipe_alignment_image", "None")}}')

    variant_calling = CannoliVariantCallingOperator(
        task_id="Variant_Calling",
        conn_id='{{ var.value.get("bio_pipe_spark_connection") }}',
        num_executors='{{ var.value.get("bio_pipe_variantcalling_spark_num_executors", "1") }}',
        executor_memory='{{ var.value.get("bio_pipe_variantcalling_spark_executor_memory","2g" )}}',
        driver_memory='{{ var.value.get("bio_pipe_variantcalling_spark_driver_memory", "2g") }}',
        cannoli_path='{{ var.value.get("bio_pipe_cannoli") }}',
        tool="freebayes",
        input='{{var.value.get("NGS_workspace")}}' + '/{{var.value.get("bio_pipe_prefix")}}_aligned.sam',
        output='{{var.value.get("NGS_workspace")}}' + '/{{var.value.get("bio_pipe_prefix")}}_variant_calling.vcf',
        reference='{{var.value.get("bio_pipe_reference")}}',
        additional_args='{{var.value.get("bio_pipe_variantcalling_additional_args", None)}}',
        tool_args='{{var.value.get("bio_pipe_variantcalling_tool_args", None)}}',
        spark_binary='{{var.value.get("spark_binary", "spark-submit")}}',
        image='{{var.value.get("bio_pipe_variantcalling_image", "None")}}')

    annotation = CannoliAnnotationOperator(
        task_id="Annotation",
        conn_id='{{ var.value.get("bio_pipe_spark_connection") }}',
        num_executors='{{ var.value.get("bio_pipe_annotation_spark_num_executors" , "1") }}',
        executor_memory='{{ var.value.get("bio_pipe_annotation_spark_executor_memory", "2g") }}',
        driver_memory='{{ var.value.get("bio_pipe_annotation_spark_driver_memory", "2g")}}',
        cannoli_path='{{ var.value.get("bio_pipe_cannoli")}}',
        output='{{var.value.get("NGS_workspace")}}' + '/{{var.value.get("bio_pipe_prefix")}}_annotation.vcf',
        input='{{var.value.get("NGS_workspace")}}' + '/{{var.value.get("bio_pipe_prefix")}}_variant_calling.vcf',
        tool='vep',
        cache='{{ var.value.get("bio_pipe_vep_cache") }}',
        additional_args='{{var.value.get("bio_pipe_annotation_additional_args", None)}}',
        tool_args='{{var.value.get("bio_pipe_annotation_tool_args", None)}}',
        spark_binary='{{var.value.get("spark_binary", "spark-submit")}}',
        image='{{var.value.get("bio_pipe_annotation_image", "None")}}')

    should_index >> move_to_hdfs >> alignment >> variant_calling >> annotation
    should_index >> index >> move_to_hdfs
