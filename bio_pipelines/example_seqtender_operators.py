
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bio_pipeline import SeqtenderAlignmentOperator

args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}


sample = '/Volumes/Samsung_T5/ngs/chr1.fq'
reference = '/Volumes/Samsung_T5/ngs/reference/genome.fa'

with DAG('seqtender', default_args=args, schedule_interval='0 0 * * *', catchup=True) as dag:
    alignement = SeqtenderAlignmentOperator(
                        jars = '/Users/dominika/bdg-seqtender-assembly-0.3-SNAPSHOT.jar',
                        master = "local",
                        executor_memory = "2g",
                        driver_memory = "1g",
                        task_id = "alignment",
                        tool = "bwa",
                        input = sample,
                        output = "/Volumes/Samsung_T5/ngs/seqtender.sam",
                        readGroup = "SM1",
                        readGroupId = "SM1",
                        interleaved = True,
                        index = reference)
