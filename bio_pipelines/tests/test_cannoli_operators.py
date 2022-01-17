import os.path
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

import test_utils.utils as utils
from airflow import DAG
from airflow.models import TaskInstance
from bio_pipelines.operators.CannoliAlignmentOperator import CannoliAlignmentOperator
from bio_pipelines.operators.CannoliAnnotationOperator import CannoliAnnotationOperator
from bio_pipelines.operators.CannoliOperator import CannoliOperator
from bio_pipelines.operators.CannoliVariantCallingOperator import CannoliVariantCallingOperator

CANNOLI_PATH = '../../tools/lib/cannoli-assembly-spark2_2.11-0.11.0-SNAPSHOT.jar'
BIOPIPELINE_PATH = '../../tools/target/scala-2.11/BioPipeline-assembly-0.2-SNAPSHOT.jar'


class TestCannoliOperators(unittest.TestCase):

    def setUp(self):
        source_path = Path(__file__).resolve()
        self.source_dir = source_path.parent
        self.absolute_tests = str(self.source_dir.absolute())
        from airflow import settings
        from airflow.models import Connection
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

    def test_CannoliAlignmentOperatorCommandBwa(self):
        index = self.absolute_tests + "/resources/bwa_index/test.fa"
        read = self.absolute_tests + "/resources/test.fq"
        bqsr = self.absolute_tests + "/resources/bqsr.vcf"
        task = CannoliAlignmentOperator(task_id="bwa_aligner", tool="bwa", output="/tmp/out.sam", id="test_id",
                                        sample=read, reference=index,
                                        snps=bqsr)

        command = " ".join(task.build_application_arguments())
        command_expected = " ".join(
            ['bwaPipeline', read, '/tmp/out.sam', '-use_docker', '-prefix', 'test_id', '-sample_id', 'test_id',
             '-index', index, '-reference', index, '-known_snps', bqsr])
        self.assertEqual(command, command_expected)

    def test_CannoliAlignmentOperatorCommandBowtie2(self):
        reference = self.absolute_tests + "/resources/bwa_index/test.fa"
        index = self.absolute_tests + "/resources/bwa_index/test"
        read = self.absolute_tests + "/resources/test.fq"
        bqsr = self.absolute_tests + "/resources/bqsr.vcf"
        task = CannoliAlignmentOperator(task_id="bowtie2_aligner", tool="bowtie2", output="/tmp/out.sam",
                                        id="test_id", sample=read, reference=reference,
                                        snps=bqsr)

        command = " ".join(task.build_application_arguments())
        command_expected = " ".join(
            ['SingleEndBowtie2Pipeline', read, '/tmp/out.sam', '-prefix', 'test_id', '-reference', reference, '-index',
             index, '-sample_id', 'test_id', '-known_snps', bqsr, '-use_docker'])
        self.assertEqual(command, command_expected)

    def test_CannoliAlignmentOperator(self):
        source_path = Path(__file__).resolve()
        source_dir = source_path.parent
        index = self.absolute_tests + "/resources/bwa_index/test.fa"
        read = self.absolute_tests + "/resources/test.fq"
        bqsr = self.absolute_tests + "/resources/bqsr.vcf"
        task = CannoliAlignmentOperator(task_id="bwa_aligner", tool="bwa", output="/tmp/out.sam", id="test_id",
                                        sample=read, reference=index,
                                        snps=bqsr)
        self.assertIsInstance(task, CannoliOperator)

    def test_CannoliVariantCallingOperatorFreebayesCommand(self):
        aligned = self.absolute_tests + "/resources/test.sam"
        reference = self.absolute_tests + "/resources/bwa_index/test.fa"
        task = CannoliVariantCallingOperator(
            task_id="variant_calling",
            tool="freebayes",
            input=aligned,
            output="/tmp/freebayes.vcf",
            reference=reference
        )

        command = " ".join(task.build_application_arguments())
        command_expected = " ".join(
            ['freebayes', aligned, '/tmp/freebayes.vcf', '-reference', reference, '-use_docker', '-single'])

        self.assertEqual(command, command_expected)

    def test_CannoliVariantCallingOperatorOperatorFreebayesCommandWithImage(self):
        aligned = self.absolute_tests + "/resources/test.sam"
        reference = self.absolute_tests + "/resources/bwa_index/test.fa"
        image = 'dummy_image'
        task = CannoliVariantCallingOperator(
            task_id="variant_calling",
            tool="freebayes",
            input=aligned,
            output="/tmp/freebayes.vcf",
            reference=reference,
            image=image
        )

        command = " ".join(task.build_application_arguments())
        command_expected = " ".join(
            ['freebayes', aligned, '/tmp/freebayes.vcf', '-reference', reference, '-use_docker', '-single', '-image',
             image])

        self.assertEqual(command, command_expected)

    def test_CannoliVariantCallingOperatorOperator(self):
        source_path = Path(__file__).resolve()
        source_dir = source_path.parent
        aligned = self.absolute_tests + "/resources/test.sam"
        reference = self.absolute_tests + "/resources/bwa_index/test.fa"
        task = CannoliVariantCallingOperator(
            task_id="variant_calling",
            tool="freebayes",
            input=aligned,
            output="/tmp/freebayes.vcf",
            reference=reference
        )

        self.assertIsInstance(task, CannoliOperator)

    def test_CannoliAnnotationOperatorVepCommand(self):
        input_vcf = self.absolute_tests + "/resources/small.vcf"
        output = "/tmp/vep.vcf"
        task = CannoliAnnotationOperator(
            task_id="annotation",
            tool="vep",
            input=input_vcf,
            output=output,
            cache="dummy_cache"
        )

        command = " ".join(['vep', input_vcf, output,
                            '-use_docker', '-single', '-cache', 'dummy_cache'])
        self.assertIsInstance(task, CannoliOperator)

        self.assertEqual(command, " ".join(task.build_application_arguments()))

    def test_CannoliAnnotationOperatorVepExecute(self):
        dag = DAG(dag_id='cannoli', start_date=datetime.now())
        input_vcf = self.absolute_tests + "/resources/small.vcf"
        out = "/tmp/vep_cannoli.vcf"
        cache = os.getenv('VEP_CACHE_98')
        try:
            cannoli_jar_path = str(
                Path(CANNOLI_PATH).resolve())
        except:
            cannoli_jar_path = os.getenv('CANNOLI_JAR')
        task = CannoliAnnotationOperator(
            task_id="annotation",
            tool="vep",
            input=input_vcf,
            output=out,
            cache=cache,
            image="quay.io/biocontainers/ensembl-vep:98.2--pl526hecc5488_0",
            dag=dag,
            cannoli_path=cannoli_jar_path,
            conn_id='spark_host_test')

        ti = TaskInstance(task=task, execution_date=datetime.now())
        ti.render_templates()
        task.execute(ti.get_template_context())
        self.assertTrue(os.path.isfile(out))
        os.remove(out)

    def test_CannoliAnnotationOperatorVepCommandCustomized(self):
        input_vcf = self.absolute_tests + "/resources/small.vcf"
        output = "/tmp/vep.vcf"
        task = CannoliAnnotationOperator(
            task_id="annotation",
            tool="vep",
            input=input_vcf,
            output=output,
            cache="dummy_cache",
            image="dummy_image",
            additional_args="--pick_allele"
        )

        command = " ".join(['vep', input_vcf, output,
                            '-use_docker', '-single', '-cache', 'dummy_cache', '-image', 'dummy_image',
                            '--pick_allele'])
        self.assertIsInstance(task, CannoliOperator)

        self.assertEqual(command, " ".join(task.build_application_arguments()))

    def test_CannoliOperatorDefault(self):
        task = CannoliOperator(task_id="cannoli", cannoli_path="cannoli")

        self.assertEqual(task.cannoli_path, "cannoli")

        self.assertTrue('spark.serializer' in task._conf)
        self.assertEqual(task._conf["spark.serializer"], "org.apache.spark.serializer.KryoSerializer")

        self.assertTrue('spark.kryo.registrator' in task._conf)
        self.assertEqual(task._conf["spark.kryo.registrator"], "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")

        self.assertEqual(task._java_class, 'org.bdgenomics.cannoli.cli.Cannoli')

        from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
        self.assertIsInstance(task, SparkSubmitOperator)

    def test_CannoliOperatorException(self):
        task = CannoliOperator(task_id="cannoli", cannoli_path="cannoli")
        with self.assertRaises(NotImplementedError):
            task.build_application_arguments()

    def test_CannoliOperatorNonDefaultWithConf(self):
        task = CannoliOperator(task_id="cannoli", cannoli_path="cannoli",
                               conf={"spark.yarn.driver.memoryOverhead": "2098"})

        self.assertTrue('spark.serializer' in task._conf)
        self.assertEqual(task._conf["spark.serializer"], "org.apache.spark.serializer.KryoSerializer")

        self.assertTrue('spark.kryo.registrator' in task._conf)
        self.assertEqual(task._conf["spark.kryo.registrator"], "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")

        self.assertTrue('spark.yarn.driver.memoryOverhead' in task._conf)
        self.assertEqual(task._conf["spark.yarn.driver.memoryOverhead"], "2098")

    def test_CannoliOperatorNonDefaultWithJavaClass(self):
        task = CannoliOperator(task_id="cannoli", cannoli_path="cannoli", java_class="BioPipeline")
        self.assertEqual(task._java_class, 'BioPipeline')

    @mock.patch('bio_pipelines.operators.CannoliOperator.CannoliOperator.build_application_arguments',
                return_value=True)
    @mock.patch('airflow.contrib.hooks.spark_submit_hook.SparkSubmitHook.submit', return_value=True)
    def test_CannoliOperatorExecute(self, build_args, spark_submit):
        dag = DAG(dag_id='cannoli', start_date=datetime.now())
        task = CannoliOperator(dag=dag, task_id="cannoli", cannoli_path="cannoli", java_class="BioPipeline")
        ti = TaskInstance(task=task, execution_date=datetime.now())
        ti.render_templates()
        task.execute(ti.get_template_context())
        build_args.assert_called_once()
        spark_submit.assert_called_once()

    def test_CannoliAlignmentOperatorExecuteBwaInterleaved(self):
        dag = DAG(dag_id='cannoli', start_date=datetime.now())
        now = datetime.now()
        date_format = now.strftime("%m_%d_%Y_%H_%M_%S")
        out = "/tmp/" + date_format + ".sam"
        index = self.absolute_tests + "/resources/bwa_index/test.fa"
        read = self.absolute_tests + "/resources/e_coli_13.ifq"
        bqsr = self.absolute_tests + "/resources/bqsr.vcf"
        try:
            cannoli_jar_path = str(
                Path(BIOPIPELINE_PATH).resolve())
        except:
            cannoli_jar_path = os.getenv('BIOPIPELINE_JAR')

        import os.path
        self.assertTrue(os.path.isfile(cannoli_jar_path))
        task = CannoliAlignmentOperator(task_id="alignment", tool="bwa", output=out, id="test_id",
                                        sample=read, reference=index, cannoli_path=cannoli_jar_path,
                                        snps=bqsr, java_class="BioPipeline", dag=dag, conn_id='spark_host_test')
        ti = TaskInstance(task=task, execution_date=datetime.now())
        ti.render_templates()
        task.execute(ti.get_template_context())
        import os.path
        self.assertTrue(os.path.isfile(out))
        os.remove(out)

    def test_CannoliAlignmentOperatorExecuteBowtie2Interleaved(self):
        dag = DAG(dag_id='cannoli', start_date=datetime.now())
        now = datetime.now()
        date_format = now.strftime("%m_%d_%Y_%H_%M_%S")
        out = "/tmp/" + date_format + ".sam"
        reference = self.absolute_tests + "/resources/bowtie2_index/e_coli_short.fa"
        read = self.absolute_tests + "/resources/e_coli_13.ifq"
        bqsr = self.absolute_tests + "/resources/bqsr.vcf"
        try:
            cannoli_jar_path = str(
                Path(BIOPIPELINE_PATH).resolve())
        except:
            cannoli_jar_path = os.getenv('BIOPIPELINE_JAR')

        import os.path
        self.assertTrue(os.path.isfile(cannoli_jar_path))
        task = CannoliAlignmentOperator(task_id="bwa", tool="bowtie2", output=out, id="test_id",
                                        sample=read, reference=reference, cannoli_path=cannoli_jar_path,
                                        snps=bqsr, java_class="BioPipeline", dag=dag, conn_id='spark_host_test')
        ti = TaskInstance(task=task, execution_date=datetime.now())
        ti.render_templates()
        task.execute(ti.get_template_context())
        import os.path
        self.assertTrue(os.path.isfile(out))
        utils.test_ecoli13(out, True)
        os.remove(out)

    def test_CannoliAlignmentOperatorExecuteBowtie2SingleEnd(self):
        dag = DAG(dag_id='cannoli', start_date=datetime.now())
        now = datetime.now()
        date_format = now.strftime("%m_%d_%Y_%H_%M_%S")
        out = "/tmp/" + date_format + ".sam"
        reference = self.absolute_tests + "/resources/bowtie2_index/e_coli_short.fa"
        read = self.absolute_tests + "/resources/e_coli_13.fq"
        bqsr = self.absolute_tests + "/resources/bqsr.vcf"
        try:
            cannoli_jar_path = str(
                Path(BIOPIPELINE_PATH).resolve())
        except:
            cannoli_jar_path = os.getenv('BIOPIPELINE_JAR')

        import os.path
        self.assertTrue(os.path.isfile(cannoli_jar_path))
        task = CannoliAlignmentOperator(task_id="bowtie2", tool="bowtie2", output=out, id="test_id",
                                        sample=read, reference=reference, cannoli_path=cannoli_jar_path,
                                        snps=bqsr, java_class="BioPipeline", dag=dag, conn_id='spark_host_test')
        ti = TaskInstance(task=task, execution_date=datetime.now())
        ti.render_templates()
        task.execute(ti.get_template_context())
        import os.path
        self.assertTrue(os.path.isfile(out))
        os.remove(out)

    def test_CannoliAlignmentOperatorExecuteFreebayes(self):
        dag = DAG(dag_id='cannoli', start_date=datetime.now())
        now = datetime.now()
        date_format = now.strftime("%m_%d_%Y_%H_%M_%S")
        out = "/tmp/" + date_format + ".vcf"
        reference = self.absolute_tests + "/resources/freebayes.fa"
        input = self.absolute_tests + "/resources/out_sort_1000.sam"
        try:
            cannoli_jar_path = str(
                Path(CANNOLI_PATH).resolve())
        except:
            cannoli_jar_path = os.getenv('CANNOLI_JAR')

        self.assertTrue(os.path.isfile(cannoli_jar_path))
        task = CannoliVariantCallingOperator(
            task_id="variant_calling",
            tool="freebayes",
            input=input,
            output=out,
            reference=reference,
            dag=dag,
            conn_id='spark_host_test',
            cannoli_path=cannoli_jar_path
        )

        ti = TaskInstance(task=task, execution_date=datetime.now())
        ti.render_templates()
        task.execute(ti.get_template_context())
        self.assertTrue(os.path.isfile(out))
        utils.test_freebayes(out)
        os.remove(out)
