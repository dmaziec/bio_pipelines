import unittest
from datetime import datetime
from unittest import mock

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.bio_pipeline import SeqtenderAlignmentOperator, SeqtenderVepOperator, \
    SeqtenderVcfToolsOperator, SeqtenderAlignmentCommandOperator, SeqtenderAnnotationCommandOperator
from bio_pipelines.operators.SeqtenderOperator import SeqtenderOperator
from test_utils.utils import test_ecoli13, test_vep, test_vcftools


class TestSeqtenderOperators(unittest.TestCase):
    def setUp(self):
        import os
        self.seqtender_path = os.getenv('SEQTENDER_JAR')

    @mock.patch('airflow.operators.bio_pipeline.SeqtenderAlignmentCommandOperator.run', return_value=True)
    def test_SeqtenderAlignmentOperator(self, SeqtenderAlignment):
        dag = DAG(dag_id='seqtender', start_date=datetime.now())
        task = SeqtenderAlignmentOperator(jars=self.seqtender_path,
                                          master="local",
                                          executor_memory="2g",
                                          driver_memory="1g",
                                          tool="bwa",
                                          input='/Volumes/Samsung_T5/ngs2/chr1.fq',
                                          output="/tmp/seqtender2.sam",
                                          readGroup="SM1",
                                          readGroupId="SM1",
                                          interleaved=True,
                                          index='/Volumes/Samsung_T5/ngs2/reference/genome.fa',
                                          dag=dag,
                                          task_id='seqtender_alignment')
        ti = TaskInstance(task=task, execution_date=datetime.now())
        ti.render_templates()
        task.execute(ti.get_template_context())
        command = "docker run --rm -i -v /Volumes/Samsung_T5/ngs2/reference/:/data " \
                  "quay.io/biocontainers/bwa:0.7.17--hed695b0_7 bwa mem -R \"@RG\\tID:SM1\\tSM1\" -p /data/genome.fa - "
        self.assertEqual(task._command, command)
        SeqtenderAlignment.assert_called_once()

    def test_SeqtenderOperatorSparkSessionConf(self):
        dag = DAG(dag_id='seqtender', start_date=datetime.now())
        task = SeqtenderOperator(master="local",
                                 jars=self.seqtender_path,
                                 executor_memory="2g",
                                 driver_memory="1g",
                                 executor_cores=2,
                                 dag=dag,
                                 num_executors=5,
                                 task_id='seqtender_alignment',
                                 conf={"spark.dynamicAllocation.enabled": "false"})
        ti = TaskInstance(task=task, execution_date=datetime.now())
        ti.render_templates()
        task.execute(ti.get_template_context())
        task.execute(ti.get_template_context())
        configurations = task.ss.sparkContext.getConf().getAll()
        print(configurations)
        self.assertTrue(('spark.driver.memory', '1g') in configurations)
        self.assertTrue(('spark.executor.cores', '2') in configurations)
        self.assertTrue(('spark.jars', self.seqtender_path) in configurations)
        self.assertTrue(('spark.master', 'local') in configurations)
        self.assertTrue(('spark.dynamicAllocation.enabled', 'false') in configurations)
        self.assertTrue(('spark.executor.instances', '5') in configurations)

    @mock.patch('pyseqtender.SeqTenderAlignment.pipe_reads', return_value=True)
    @mock.patch('pyseqtender.SeqTenderAlignment.save_reads', return_value=True)
    def test_SeqtenderAlignmentCommandOperator(self, Piper, Saver):
        dag = DAG(dag_id='seqtender', start_date=datetime.now())
        task = SeqtenderAlignmentCommandOperator(jars=self.seqtender_path,
                                                 master="local",
                                                 executor_memory="2g",
                                                 driver_memory="1g",
                                                 input='/Volumes/Samsung_T5/ngs2/chr1.fq',
                                                 output="/tmp/seqtender2.sam",
                                                 command="docker run --rm -i -v /Volumes/Samsung_T5/ngs2/reference/:/data " \
                                                         "quay.io/biocontainers/bwa:0.7.17--hed695b0_7 bwa mem -R \"@RG\\tID:SM1\\tSM1\" -p /data/genome.fa - ",
                                                 dag=dag,
                                                 task_id='seqtender_alignment')

        ti = TaskInstance(task=task, execution_date=datetime.now())
        ti.render_templates()
        task.execute(ti.get_template_context())
        command = "docker run --rm -i -v /Volumes/Samsung_T5/ngs2/reference/:/data " \
                  "quay.io/biocontainers/bwa:0.7.17--hed695b0_7 bwa mem -R \"@RG\\tID:SM1\\tSM1\" -p /data/genome.fa - "
        self.assertEqual(task._command, command)
        Piper.assert_called_once()
        Saver.assert_called_once()

    @mock.patch('airflow.operators.bio_pipeline.SeqtenderAnnotationCommandOperator.run', return_value=True)
    def test_SeqtenderVepTool(self, SeqtenderAnnotation):
        dag = DAG(dag_id='seqtender', start_date=datetime.now())
        task = SeqtenderVepOperator(
            jars=self.seqtender_path,
            master="local",
            executor_memory="2g",
            driver_memory="1g",
            task_id="annotation_vep",
            input="/Volumes/Samsung_T5/ngs2/chr1variant_calling.vcf",
            output="/tmp/annotation_vep2.vcf",
            image="quay.io/biocontainers/ensembl-vep:98.2--pl526hecc5488_0",
            cache_dir="/Volumes/Samsung_T5/ngs/vep/98",
            dag=dag)
        ti = TaskInstance(task=task, execution_date=datetime.now())
        ti.render_templates()
        task.execute(ti.get_template_context())
        command = "docker run --rm -i -v /Volumes/Samsung_T5/ngs/vep:/Volumes/Samsung_T5/ngs/vep " \
                  "quay.io/biocontainers/ensembl-vep:98.2--pl526hecc5488_0 vep --format vcf --output_file STDOUT " \
                  "--no_stats --offline --dir_cache /Volumes/Samsung_T5/ngs/vep/98 --vcf"
        self.assertEqual(task._command, command)
        SeqtenderAnnotation.assert_called_once()

    @mock.patch('airflow.operators.bio_pipeline.SeqtenderAnnotationCommandOperator.run', return_value=True)
    def test_SeqtenderVCFTools(self, SeqtenderAnnotation):
        dag = DAG(dag_id='seqtender', start_date=datetime.now())

        task = SeqtenderVcfToolsOperator(
            jars=self.seqtender_path,
            master="local",
            task_id="vcftools",
            executor_memory="2g",
            driver_memory="1g",
            additional_args=["--minQ 20 --recode --recode-INFO-all"],
            input="/Volumes/Samsung_T5/ngs2/chr1variant_calling.vcf",
            output="/tmp/vcftools.vcf",
            dag=dag)
        ti = TaskInstance(task=task, execution_date=datetime.now())
        ti.render_templates()
        task.execute(ti.get_template_context())
        command = "docker run --rm -i -v /tmp:/tmp biocontainers/vcftools:v0.1.16-1-deb_cv1 vcftools --vcf - --minQ " \
                  "20 --recode --recode-INFO-all --stdout"
        self.assertEqual(task._command, command)
        SeqtenderAnnotation.assert_called_once()

    @mock.patch('pyseqtender.SeqTenderAnnotation.pipe_variants', return_value=True)
    @mock.patch('pyseqtender.SeqTenderAnnotation.save_variants', return_value=True)
    def SeqtenderAnnotationCommandOperator(self, Piper, Saver):
        dag = DAG(dag_id='seqtender', start_date=datetime.now())
        task = SeqtenderAnnotationCommandOperator(jars=self.seqtender_path,
                                                  master="local",
                                                  executor_memory="2g",
                                                  driver_memory="1g",
                                                  input="/Volumes/Samsung_T5/ngs2/chr1variant_calling.vcf",
                                                  output="/tmp/vcftools.vcf",
                                                  command='docker run --rm -i -v /tmp:/tmp '
                                                          'biocontainers/vcftools:v0.1.16-1-deb_cv1 vcftools --vcf - '
                                                          '--minQ ' \
                                                          '20 --recode --recode-INFO-all --stdout',
                                                  dag=dag,
                                                  task_id='seqtender_alignment')

        ti = TaskInstance(task=task, execution_date=datetime.now())
        ti.render_templates()
        task.execute(ti.get_template_context())
        command = 'docker run --rm -i -v /tmp:/tmp biocontainers/vcftools:v0.1.16-1-deb_cv1 vcftools --vcf - ' \
                  '--minQ ' \
                  '20 --recode --recode-INFO-all --stdout'
        self.assertEqual(task._command, command)
        Piper.assert_called_once()
        Saver.assert_called_once()

    def test_SeqtenderAlignmentCommandOperatorExecute(self):
        dag = DAG(dag_id='seqtender', start_date=datetime.now())
        from pathlib import Path
        source_path = Path(__file__).resolve()
        source_dir = source_path.parent
        folder = str(source_dir.absolute()) + "/resources/bwa_index/"
        read = str(source_dir.absolute()) + "/resources/e_coli_13.ifq"
        index = "/data/test.fa"
        now = datetime.now()
        date_format = now.strftime("%m_%d_%Y_%H_%M_%S")
        out = "/tmp/" + date_format + ".bam"
        task = SeqtenderAlignmentCommandOperator(jars=self.seqtender_path,
                                                 master="local",
                                                 executor_memory="2g",
                                                 driver_memory="1g",
                                                 input=read,
                                                 output=out,
                                                 command="docker run --rm -i -v " + folder + ":/data " \
                                                                                             "quay.io/biocontainers/bwa:0.7.17--hed695b0_7 bwa mem -R \"@RG\\tID:test_id\\tSM:test_id\\tLB:test_id\\tPL:ILLUMINA\\tPU:0\" -p " + index + " - ",
                                                 dag=dag,
                                                 task_id='seqtender_alignment')
        ti = TaskInstance(task=task, execution_date=datetime.now())
        ti.render_templates()
        task.execute(ti.get_template_context())
        import os.path
        self.assertTrue(os.path.isfile(out))
        self.assertTrue(os.path.isfile(out + ".bai"))
        self.assertTrue(os.path.isfile(out + ".sbi"))
        test_ecoli13(out, False, True)
        os.remove(out)
        os.remove(out + ".sbi")
        os.remove(out + ".bai")
        # os.remove(str(source_dir.absolute()) + "/." + date_format + ".bam.bai.crc")
        # os.remove(str(source_dir.absolute()) + "/." + date_format + ".bam.sbi.crc")
        # os.remove(str(source_dir.absolute()) + "/." + date_format + ".bam.crc")

    def test_SeqtenderAnnotationOperatorExecute(self):
        from pathlib import Path
        source_path = Path(__file__).resolve()
        source_dir = source_path.parent
        input_vcf = str(source_dir.absolute()) + "/resources/small.vcf"
        now = datetime.now()
        out = "/tmp/" + now.strftime("%m_%d_%Y_%H_%M_%S") + ".vcf"
        dag = DAG(dag_id='seqtender', start_date=datetime.now())
        task = SeqtenderAnnotationCommandOperator(jars=self.seqtender_path,
                                                  master="local",
                                                  executor_memory="2g",
                                                  driver_memory="1g",
                                                  input=input_vcf,
                                                  output=out,
                                                  command='docker run --rm -i -v /tmp:/tmp '
                                                          'biocontainers/vcftools:v0.1.16-1-deb_cv1 vcftools --vcf - '
                                                          '--minQ ' \
                                                          '2486 --recode --recode-INFO-all --stdout',
                                                  dag=dag,
                                                  task_id='seqtender_alignment')

        ti = TaskInstance(task=task, execution_date=datetime.now())
        ti.render_templates()
        task.execute(ti.get_template_context())
        import os.path
        self.assertTrue(os.path.isfile(out))
        # os.remove(out)

    def test_SeqtenderVCFToolsExecute(self):
        from pathlib import Path
        source_path = Path(__file__).resolve()
        source_dir = source_path.parent
        input_vcf = str(source_dir.absolute()) + "/resources/small.vcf"
        now = datetime.now()
        out = "/tmp/" + now.strftime("%m_%d_%Y_%H_%M_%S") + ".vcf"
        dag = DAG(dag_id='seqtender', start_date=datetime.now())

        task = SeqtenderVcfToolsOperator(
            jars=self.seqtender_path,
            master="local",
            task_id="vcftools",
            executor_memory="2g",
            driver_memory="1g",
            additional_args=["--minQ 2486 --recode --recode-INFO-all"],
            input=input_vcf,
            output=out,
            dag=dag)
        ti = TaskInstance(task=task, execution_date=datetime.now())
        ti.render_templates()
        task.execute(ti.get_template_context())
        import os
        self.assertTrue(os.path.isfile(out))
        test_vcftools(out)
        os.remove(out)

    def test_SeqtenderVepToolExecute(self):
        dag = DAG(dag_id='seqtender', start_date=datetime.now())
        from pathlib import Path
        source_path = Path(__file__).resolve()
        source_dir = source_path.parent
        input_vcf = str(source_dir.absolute()) + "/resources/small.vcf"
        import os
        cache = os.getenv('VEP_CACHE_98')
        out = "/tmp/vep.vcf"
        task = SeqtenderVepOperator(
            jars=self.seqtender_path,
            master="local",
            executor_memory="2g",
            driver_memory="1g",
            task_id="annotation_vep",
            input=input_vcf,
            output=out,
            image="quay.io/biocontainers/ensembl-vep:98.2--pl526hecc5488_0",
            cache_dir=cache,
            dag=dag)
        ti = TaskInstance(task=task, execution_date=datetime.now())
        ti.render_templates()
        task.execute(ti.get_template_context())
        self.assertTrue(os.path.isfile(out))
        test_vep(out)
        os.remove(out)

    def test_SeqtenderVepToolCustomizedTool(self):
        dag = DAG(dag_id='seqtender', start_date=datetime.now())
        input_vcf = "home/resources/small.vcf"
        output = "/tmp/vep.vcf"
        task = SeqtenderVepOperator(
            jars=self.seqtender_path,
            master="local",
            executor_memory="2g",
            driver_memory="1g",
            task_id="annotation_vep",
            input=input_vcf,
            output=output,
            image="quay.io/biocontainers/ensembl-vep:98.2--pl526hecc5488_0",
            cache_dir="/Volumes/Samsung_T5/ngs/vep/98",
            additional_args=["--pick_allele"],
            dag=dag)

        command = "docker run --rm -i -v /Volumes/Samsung_T5/ngs/vep:/Volumes/Samsung_T5/ngs/vep quay.io/biocontainers/ensembl-vep:98.2--pl526hecc5488_0 vep --format vcf --output_file STDOUT --no_stats --offline --dir_cache /Volumes/Samsung_T5/ngs/vep/98 --vcf --pick_allele"
        self.assertEqual(command, task.build_application_arguments())

    def test_SeqtenderVCFToolsWithImage(self):
        dag = DAG(dag_id='seqtender', start_date=datetime.now())

        task = SeqtenderVcfToolsOperator(
            jars=self.seqtender_path,
            master="local",
            task_id="vcftools",
            executor_memory="2g",
            driver_memory="1g",
            additional_args=["--minQ 20 --recode --recode-INFO-all"],
            input="/Volumes/Samsung_T5/ngs2/chr1variant_calling.vcf",
            output="/tmp/vcftools.vcf",
            dag=dag,
            image="dummy_image")
        command = "docker run --rm -i -v /tmp:/tmp dummy_image vcftools --vcf - --minQ " \
                  "20 --recode --recode-INFO-all --stdout"
        self.assertEqual(task.build_application_arguments(), command)

# suite = unittest.TestLoader().loadTestsFromTestCase(TestSeqtenderOperators)
# unittest.TextTestRunner(verbosity=2).run(suite)
