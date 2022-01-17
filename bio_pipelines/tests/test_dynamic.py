import unittest

from airflow import settings
from bio_pipelines.helpers.cannoli_commands_args import *

session = settings.Session()
# export AIRFLOW__CORE__UNIT_TEST_MODE=true
from pathlib import Path
from airflow.models import Variable

source_path = Path(__file__).resolve()
source_dir = source_path.parent
"""" THESE TESTS ARE IMPLEMENTED FOR THE DYNAMIC BASED PIPELINE """
"""class TestBioDNASeqDAG(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        Variable.set("bio_pipe_aligner", "bowtie2")
        Variable.set("NGS_workspace", str(source_dir.absolute()) + "/resources/ngs_workspace_1/")
        cls.dagbag = DagBag()

    def test_dag_loaded(self):
        dag = self.dagbag.get_dag(dag_id='BioDNASeq')
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 29)

    def test_dependencies_move_to_hdfs(self):
        dag = self.dagbag.get_dag("BioDNASeq")
        move_to_hdfs_task = dag.get_task('move_fromLocal_toHdfs')
        upstream_task_ids = list(map(lambda task: task.task_id, move_to_hdfs_task.upstream_list))
        upstream_task_ids.sort()
        expected = ['ecoli_fastq_interleaver', 'index', 'should_index']
        expected.sort()
        self.assertListEqual(upstream_task_ids, expected)
        downstream_task_ids = list(map(lambda task: task.task_id, move_to_hdfs_task.downstream_list))
        downstream_task_ids.sort()
        self.assertListEqual(downstream_task_ids,
                             ['Alignment_1234', 'Alignment_ch2', 'Alignment_chr1', 'Alignment_ecoli',
                              'Alignment_homo_sapiens', 'Alignment_read'])

    def test_interleaver(self):
        interelavers = ['ecoli_fastq_interleaver']
        dag = self.dagbag.get_dag("BioDNASeq")
        for i in interelavers:
            interleaver = dag.get_task(i)
            upstream_task_ids = list(map(lambda task: task.task_id, interleaver.upstream_list))
            upstream_task_ids.sort()
            self.assertListEqual(upstream_task_ids, [])

            downstream_task_ids = list(map(lambda task: task.task_id, interleaver.downstream_list))
            downstream_task_ids.sort()
            self.assertListEqual(downstream_task_ids, ['move_fromLocal_toHdfs'])

    def indexing(self):
        dag = self.dagbag.get_dag("BioDNASeq")
        indexing = dag.get_task('should_index')
        upstream_task_ids = list(map(lambda task: task.task_id, indexing.upstream_list))
        upstream_task_ids.sort()
        self.assertListEqual(upstream_task_ids, [])

        downstream_task_ids = list(map(lambda task: task.task_id, indexing.downstream_list))
        downstream_task_ids.sort()
        self.assertListEqual(downstream_task_ids, ['move_fromLocal_toHdfs', 'should_index'])

        index = dag.get_task('index')
        upstream_task_ids = list(map(lambda task: task.task_id, index.upstream_list))
        upstream_task_ids.sort()
        self.assertListEqual(upstream_task_ids, ['should_index'])

        downstream_task_ids = list(map(lambda task: task.task_id, index.downstream_list))
        downstream_task_ids.sort()
        self.assertListEqual(downstream_task_ids, ['move_fromLocal_toHdfs'])

    def test_alignment(self):
        dag = self.dagbag.get_dag("BioDNASeq")

        common = ['1234', 'ch2', 'chr1', 'homo_sapiens', 'read', 'ecoli']
        for operator in common:
            alignment = dag.get_task('Alignment_' + operator)
            upstream_task_ids = list(map(lambda task: task.task_id, alignment.upstream_list))
            upstream_task_ids.sort()
            self.assertListEqual(upstream_task_ids, ['move_fromLocal_toHdfs'])

            downstream_task_ids = list(map(lambda task: task.task_id, alignment.downstream_list))
            downstream_task_ids.sort()
            self.assertListEqual(downstream_task_ids, ['VariantCalling_' + operator])

    def test_variant_calling(self):
        dag = self.dagbag.get_dag("BioDNASeq")

        common = ['1234', 'ch2', 'chr1', 'homo_sapiens', 'read', 'ecoli']
        for operator in common:
            alignment = dag.get_task('VariantCalling_' + operator)
            upstream_task_ids = list(map(lambda task: task.task_id, alignment.upstream_list))
            upstream_task_ids.sort()
            self.assertListEqual(upstream_task_ids, ['Alignment_' + operator])

            downstream_task_ids = list(map(lambda task: task.task_id, alignment.downstream_list))
            downstream_task_ids.sort()
            self.assertListEqual(downstream_task_ids, ['Annotation_' + operator])

    def test_annotation(self):
        dag = self.dagbag.get_dag("BioDNASeq")
        common = ['1234', 'ch2', 'chr1', 'homo_sapiens', 'read', 'ecoli']
        for operator in common:
            alignment = dag.get_task('Annotation_' + operator)
            upstream_task_ids = list(map(lambda task: task.task_id, alignment.upstream_list))
            upstream_task_ids.sort()
            self.assertListEqual(upstream_task_ids, ['VariantCalling_' + operator])

            downstream_task_ids = list(map(lambda task: task.task_id, alignment.downstream_list))
            downstream_task_ids.sort()
            self.assertListEqual(downstream_task_ids, [])

    def test_if_should_index(self):
        dag = self.dagbag.get_dag("BioDNASeq")
        should_index = dag.get_task('should_index')
        ret = should_index.python_callable(*should_index.op_args, **should_index.op_kwargs)
        self.assertEqual(ret, ['index', 'move_fromLocal_toHdfs'])

    def test_index_bowtie2_aligner_command(self):
        bowtie2_aligner = Bowtie2Aligner(
            reference=str(source_dir.absolute()) + '/resources/ngs_workspace_1/reference/reference.fa')
        command = bowtie2_aligner.get_docker_index_command()
        self.assertEqual(command, 'bowtie2-build ' + str(
            source_dir.absolute()) + "/resources/ngs_workspace_1/reference/reference.fa " + str(
            source_dir.absolute()) + "/resources/ngs_workspace_1/reference/reference" + " && mkdir -p " + str(
            source_dir.absolute()) + "/resources/ngs_workspace_1/reference/reference")


class test_ngs_workspace_2(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        Variable.set("bio_pipe_aligner", "bowtie2")
        Variable.set("NGS_workspace", str(source_dir.absolute()) + "/resources/ngs_workspace_2/")
        Variable.set('bio_pipe_reference', str(source_dir.absolute()) + '/reference/HLA_DQB1_05_01_01_02.fa')
        cls.dagbag = DagBag()

    def test_pipeline_workspace_2(self):
        self.assertEqual(Variable.get("bio_pipe_aligner"), "bowtie2")
        self.assertEqual(Variable.get("NGS_workspace"), str(source_dir.absolute()) + "/resources/ngs_workspace_2/")
        dag = self.dagbag.get_dag(dag_id='BioDNASeq')
        from airflow import settings
        from airflow.models import Connection
        from airflow.utils.db import merge_conn
        session = settings.Session()
        merge_conn(Connection(conn_id='cdh00', conn_type='spark',
                              host='local[*]'), session)
        self.assertEqual(len(dag.tasks), 9)


    @mock.patch('airflow.contrib.hooks.spark_submit_hook.SparkSubmitHook.submit', return_value=True)
    def test_alignment_operator(self, SparkMocker):
        dag = self.dagbag.get_dag(dag_id='BioDNASeq')
        alignment = dag.get_task('Alignment_proper_pairs')
        ti = TaskInstance(alignment, datetime.now())
        ti.render_templates()
        assert isinstance(alignment, airflow.contrib.operators.spark_submit_operator.SparkSubmitOperator)
        spark_conn = "cdh00"
        self.assertEqual(Variable.get('bio_pipe_spark_connection'), spark_conn)

        num_executors_actual = ti.task._num_executors
        num_executors_db = Variable.get('bio_pipe_alignment_spark_num_executors')
        self.assertEqual(num_executors_actual, num_executors_db)

        driver_memory_actual = ti.task._driver_memory
        driver_memory_db = Variable.get('bio_pipe_alignment_spark_driver_memory')
        self.assertEqual(driver_memory_actual, driver_memory_db)

        application_path_actual = ti.task._application
        application_path_db = Variable.get('bio_pipe_jar')
        self.assertEqual(application_path_actual, application_path_db)

        self.assertEqual(ti.task._conn_id, spark_conn)
        self.assertEqual(ti.task._java_class, "BioPipeline")

        sti = TaskInstance(task=alignment, execution_date=datetime.now())
        alignment.execute(sti)
        SparkMocker.assert_called_once()

        workspace = Variable.get('NGS_workspace')
        reference_dir_expected = str(source_dir.absolute()) + '/reference/HLA_DQB1_05_01_01_02.fa'
        reference_index_expected = str(source_dir.absolute()) + '/reference/HLA_DQB1_05_01_01_02'
        snps = Variable.get("bio_pipe_snps_file")
        application_args_expected = ['bowtie2Pipeline', workspace + 'proper_pairs.ifq',
                                     workspace + 'proper_pairs_aligned.sam', '-prefix', 'proper_pairs', '-reference',
                                     reference_dir_expected, '-index', reference_index_expected, '-sample_id',
                                     'proper_pairs', '-known_snps', snps, '-use_docker']
        application_args_actual = ti.task._application_args

        self.assertEqual(application_args_actual, application_args_expected)

    @mock.patch('airflow.contrib.hooks.spark_submit_hook.SparkSubmitHook.submit', return_value=True)
    def test_variant_calling_operator(self, SparkMocker):
        dag = self.dagbag.get_dag(dag_id='BioDNASeq')
        vc = dag.get_task('VariantCalling_proper_pairs')
        ti = TaskInstance(vc, datetime.now())
        ti.render_templates()
        assert isinstance(vc, airflow.contrib.operators.spark_submit_operator.SparkSubmitOperator)
        spark_conn = "cdh00"
        self.assertEqual(Variable.get('bio_pipe_spark_connection'), spark_conn)

        num_executors_actual = ti.task._num_executors
        num_executors_db = Variable.get('bio_pipe_variantcalling_spark_num_executors')
        self.assertEqual(num_executors_actual, num_executors_db)

        driver_memory_actual = ti.task._driver_memory
        driver_memory_db = Variable.get('bio_pipe_variantcalling_spark_driver_memory')
        self.assertEqual(driver_memory_actual, driver_memory_db)

        application_path_actual = ti.task._application
        application_path_db = Variable.get('bio_pipe_cannoli')
        self.assertEqual(application_path_actual, application_path_db)

        self.assertEqual(ti.task._conn_id, spark_conn)
        self.assertEqual(ti.task._java_class, "org.bdgenomics.cannoli.cli.Cannoli")

        sti = TaskInstance(task=vc, execution_date=datetime.now())
        vc.execute(sti)
        SparkMocker.assert_called_once()

        workspace = Variable.get('NGS_workspace')
        reference_dir_expected = str(source_dir.absolute()) + '/reference/HLA_DQB1_05_01_01_02.fa'
        application_args_expected = ['freebayes', workspace + 'proper_pairs_aligned.sam',
                                     workspace + 'proper_pairsvariant_calling.vcf', '-reference',
                                     reference_dir_expected, '-use_docker', '-single']
        application_args_actual = ti.task._application_args
        self.assertEqual(application_args_actual, application_args_expected)

    @mock.patch('airflow.contrib.hooks.spark_submit_hook.SparkSubmitHook.submit', return_value=True)
    def test_annotation_operator(self, SparkMocker):
        dag = self.dagbag.get_dag(dag_id='BioDNASeq')
        ann = dag.get_task('Annotation_proper_pairs')
        ti = TaskInstance(ann, datetime.now())
        ti.render_templates()
        assert isinstance(ann, airflow.contrib.operators.spark_submit_operator.SparkSubmitOperator)
        spark_conn = "cdh00"
        self.assertEqual(Variable.get('bio_pipe_spark_connection'), spark_conn)

        num_executors_actual = ti.task._num_executors
        num_executors_db = Variable.get('bio_pipe_annotation_spark_num_executors')
        self.assertEqual(num_executors_actual, num_executors_db)

        driver_memory_actual = ti.task._driver_memory
        driver_memory_db = Variable.get('bio_pipe_annotation_spark_driver_memory')
        self.assertEqual(driver_memory_actual, driver_memory_db)

        application_path_actual = ti.task._application
        application_path_db = Variable.get('bio_pipe_cannoli')
        self.assertEqual(application_path_actual, application_path_db)

        self.assertEqual(ti.task._conn_id, spark_conn)
        self.assertEqual(ti.task._java_class, "org.bdgenomics.cannoli.cli.Cannoli")

        sti = TaskInstance(task=ann, execution_date=datetime.now())
        ann.execute(sti)
        SparkMocker.assert_called_once()

        workspace = Variable.get('NGS_workspace')
        cache_workspace = Variable.get("bio_pipe_vep_cache")
        reference_dir_expected = str(source_dir.absolute()) + '/reference/HLA_DQB1_05_01_01_02.fa'
        application_args_expected = ['vep', workspace + 'proper_pairsvariant_calling.vcf',
                                     workspace + 'proper_pairsannotation.vcf', '-use_docker', '-single', '-cache',
                                     cache_workspace]
        application_args_actual = ti.task._application_args
        self.assertEqual(application_args_actual, application_args_expected)

class test_Samples(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        Variable.set("NGS_workspace", str(source_dir.absolute()) + "/resources/ngs_workspace_1/")
        cls.samples = Samples(str(source_dir.absolute()) + "/resources/ngs_workspace_1/")

    def test_set_single_interleaved(self):
        self.samples.set_samples()
        directory = str(source_dir.absolute()) + "/resources/ngs_workspace_1/"
        expected_result = [
            ('1234', [directory + '1234.fq']),
            ('ch2', [directory + 'ch2.fastq']),
            ('chr1', [directory + 'chr1.ifq']),
            ("homo_sapiens", [directory + 'homo_sapiens.fq']),
            ("read", [directory + 'read.fq'])
        ]
        expected_result_sorted = sorted(expected_result, key=lambda x: x[0])
        actual_sorted = sorted(self.samples.single_interleaved, key=lambda x: x[0])
        self.assertCountEqual(expected_result_sorted, actual_sorted)
        self.assertListEqual(expected_result_sorted, actual_sorted)

        actual_getter = self.samples.get_samples()
        actual_getter_sorted = sorted(actual_getter, key=lambda x: x[0])
        self.assertCountEqual(expected_result_sorted, actual_getter_sorted)
        self.assertListEqual(expected_result_sorted, actual_getter_sorted)

    def test_set_paired_end_samples(self):
        self.samples.set_paired_end_samples()
        directory = str(source_dir.absolute()) + "/resources/ngs_workspace_1/"
        expected_result = [
            ('ecoli', [directory + 'ecoli_1.fq', directory + 'ecoli_2.fq'])
        ]
        expected_result_sorted = sorted(expected_result, key=lambda x: x[0])
        actual_sorted = sorted(self.samples.paired, key=lambda x: x[0])
        self.assertCountEqual(expected_result_sorted, actual_sorted)
        self.assertListEqual(expected_result_sorted, actual_sorted)

        actual_getter = self.samples.get_paired_samples()
        actual_getter_sorted = sorted(actual_getter, key=lambda x: x[0])
        self.assertCountEqual(expected_result_sorted, actual_getter_sorted)
        self.assertListEqual(expected_result_sorted, actual_getter_sorted)

    def test_get_all_files_grouped(self):
        directory = str(source_dir.absolute()) + "/resources/ngs_workspace_1/"
        expected_result = [
            ('1234', [directory + '1234.fq']),
            ('ch2', [directory + 'ch2.fastq']),
            ('chr1', [directory + 'chr1.ifq']),
            ("homo_sapiens", [directory + 'homo_sapiens.fq']),
            ("read", [directory + 'read.fq']),
            ('ecoli', [directory + 'ecoli.ifq'])
        ]

        expected_sorted = sorted(expected_result, key=lambda x: x[0])
        actual = self.samples.get_pipeline_samples()
        actual_sorted = sorted(actual, key=lambda x: x[0])
        self.assertCountEqual(expected_sorted, actual_sorted)
        self.assertListEqual(expected_sorted, actual_sorted)

    def test_generate_interleaved_fastq_file_name(self):
        correct = []
        correct += [("chr1_1.fq", "chr1_2.fq")]
        correct += [("chr1_1.fastq", "chr1_2.fastq")]
        expected_res_correct = "chr1.ifq"

        wrong = []
        wrong += [("chr1.fq", "chr1.fq")]
        wrong += [("chr1.fastq", "chr1.fq")]
        wrong += [("chr1_1.fq", "chr_1.fastq")]
        expected_res_wrong = None

        for element in correct:
            ret = self.samples.generate_interleaved_fastq_file_name(element[0], element[1])
            self.assertEqual(ret, expected_res_correct,
                             "should return " + str(expected_res_correct) + " for " + element[0] + " " + element[
                                 1] + " returned " + str(ret))

        for element in wrong:
            ret = self.samples.generate_interleaved_fastq_file_name(element[0], element[1])
            self.assertEqual(ret, expected_res_wrong,
                             "should return None for {0} and {1} return {2}".format(element[0], element[1], str(ret)))

"""

"""" THESE TESTS ARE COMMON FOR BOTH STATIC AND DYNAMIC"""
class test_Aligner(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        reference_path = str(source_dir.absolute()) + "/resources/ngs_workspace_1/reference/reference.fa"
        cls.aligner_correct = Aligner(reference=reference_path, docker_image="dummy")

    def test_correct_reference_prefixes(self):
        reference_path = str(source_dir.absolute()) + "/resources/ngs_workspace_1/reference/reference.fa"
        self.assertEqual(self.aligner_correct.reference, reference_path)
        reference_path_wthout_ext = str(source_dir.absolute()) + "/resources/ngs_workspace_1/reference/reference"
        self.assertEqual(self.aligner_correct.reference_without_ext, reference_path_wthout_ext)
        self.assertEqual(self.aligner_correct.index, 'reference.fa')

    def test_get_aligner_function(self):
        reference_path = str(source_dir.absolute()) + "/resources/ngs_workspace_1/reference/reference.fa"
        Variable.set("bio_pipe_aligner", "bowtie2")
        aligner_str = "bowtie2"
        aligner = get_aligner()
        assert isinstance(aligner, Bowtie2Aligner)
        Variable.set("bio_pipe_aligner", "bwa")
        aligner_str = "bwa"
        aligner = get_aligner()
        assert isinstance(aligner, BwaAligner)


class test_Bowtie2Aligner(unittest.TestCase):
    def test_index_checker(self):
        bowtie2_aligner = Bowtie2Aligner(
            reference=str(source_dir.absolute()) + "/resources/ngs_workspace_1/reference/reference.fa")
        if_index = bowtie2_aligner.index_exists()
        self.assertFalse(if_index)

    def test_docker_index_command(self):
        reference = str(source_dir.absolute()) + "/resources/ngs_workspace_1/reference/HLA_DQB1_05_01_01_02.fa"
        reference_without_ext = str(source_dir.absolute()) + "/resources/ngs_workspace_1/reference/HLA_DQB1_05_01_01_02"
        bowtie2_aligner = Bowtie2Aligner(reference=reference)
        expected_docker_command = "bowtie2-build " + reference + " " + reference_without_ext + " && touch " + reference_without_ext
        actual_docker_command = bowtie2_aligner.get_docker_index_command()
        self.assertEqual(expected_docker_command, actual_docker_command)

    def test_application_args(self):
        reference = "path/to/reference/reference.fa"
        reference_without_ext = "path/to/reference/reference"
        id = "chr1"
        sample = "/path/to/sample/sample.fq"
        location = "/path/to/sample/"
        bowtie2_aligner = Bowtie2Aligner(reference=reference)
        snps = Variable.get("bio_pipe_snps_file")
        expected_args = ['SingleEndBowtie2Pipeline',
                         sample,
                         location + id + "_aligned.sam",
                         "-prefix", id,
                         '-reference', reference,
                         '-index', reference_without_ext,
                         '-sample_id', id,
                         '-known_snps', snps,
                         '-use_docker']

        expected_args_str = " ".join(expected_args)
        output = location + id + "_aligned.sam"
        actual_args_str = " ".join(bowtie2_aligner.alignment(id, sample, output, snps))

        self.assertEqual(expected_args_str, actual_args_str)

        sample_interleaved = "/path/to/sample/sample.ifq"
        expected_args = ['bowtie2Pipeline',
                         sample_interleaved,
                         location + id + "_aligned.sam",
                         "-prefix", id,
                         '-reference', reference,
                         '-index', reference_without_ext,
                         '-sample_id', id,
                         '-known_snps', snps,
                         '-use_docker']
        expected_args_str = " ".join(expected_args)
        actual_args_str = " ".join(
            bowtie2_aligner.alignment(id, sample_interleaved, location + id + "_aligned.sam", snps))

        self.assertEqual(expected_args_str, actual_args_str)

    def test_application_args_additional_args(self):
        reference = "path/to/reference/reference.fa"
        reference_without_ext = "path/to/reference/reference"
        id = "chr1"
        sample = "/path/to/sample/sample.fq"
        location = "/path/to/sample/"
        bowtie2_aligner = Bowtie2Aligner(reference=reference)
        snps = Variable.get("bio_pipe_snps_file")
        additional = '"-bowtie2_args --phred33"'
        expected_args = ['SingleEndBowtie2Pipeline',
                         sample,
                         location + id + "_aligned.sam",
                         "-prefix", id,
                         '-reference', reference,
                         '-index', reference_without_ext,
                         '-sample_id', id,
                         '-known_snps', snps,
                         '-use_docker',
                         additional]

        expected_args_str = " ".join(expected_args)
        output = location + id + "_aligned.sam"

        actual_args_str = " ".join(bowtie2_aligner.alignment(id, sample, output, snps, [additional]))
        print(actual_args_str)
        self.assertEqual(expected_args_str, actual_args_str)

        sample_interleaved = "/path/to/sample/sample.ifq"
        expected_args = ['bowtie2Pipeline',
                         sample_interleaved,
                         location + id + "_aligned.sam",
                         "-prefix", id,
                         '-reference', reference,
                         '-index', reference_without_ext,
                         '-sample_id', id,
                         '-known_snps', snps,
                         '-use_docker']
        expected_args_str = " ".join(expected_args)
        actual_args_str = " ".join(
            bowtie2_aligner.alignment(id, sample_interleaved, location + id + "_aligned.sam", snps))

        self.assertEqual(expected_args_str, actual_args_str)


class test_BwaAligner(unittest.TestCase):
    def test_index_checker(self):
        bwa_aligner = BwaAligner(
            reference=str(source_dir.absolute()) + "/resources/ngs_workspace_1/reference/reference/reference.fa")
        if_index = bwa_aligner.index_exists()
        self.assertTrue(if_index)

        bwa_aligner = BwaAligner(
            reference=str(source_dir.absolute()) + "/resources/ngs_workspace_1/reference/HLA_DQB1_05_01_01_02.fa")
        if_index = bwa_aligner.index_exists()
        self.assertFalse(if_index)

    def test_docker_index_command(self):
        reference = str(source_dir.absolute()) + "/resources/ngs_workspace_1/reference/HLA_DQB1_05_01_01_02.fa"
        reference_id = "HLA_DQB1_05_01_01_02"
        reference_without_ext = str(source_dir.absolute()) + "/resources/ngs_workspace_1/reference/HLA_DQB1_05_01_01_02"
        reference_location = str(source_dir.absolute()) + "/resources/ngs_workspace_1/reference"
        bwa_aligner = BwaAligner(reference=reference)
        expected_docker_command = "/bin/bash -c \"cd " + reference_location + " && bwa index " + reference + "\""
        actual_docker_command = bwa_aligner.get_docker_index_command()
        self.assertEqual(expected_docker_command, actual_docker_command)

    def test_application_args(self):
        reference = "path/to/reference/reference.fa"
        reference_without_ext = "path/to/reference/reference"
        id = "chr1"
        sample = "/path/to/sample/sample.fq"
        location = "/path/to/sample/"
        bwa_aligner = BwaAligner(reference=reference)
        snps = Variable.get("bio_pipe_snps_file")
        expected_args = ['bwaPipeline',
                         sample,
                         location + id + "_aligned.sam",
                         '-use_docker',
                         '-prefix',
                         id, '-sample_id', id, '-index', reference,
                         '-reference', reference, '-known_snps', snps]
        expected_args_str = " ".join(expected_args)
        output = location + id + "_aligned.sam"
        actual_args_str = " ".join(bwa_aligner.alignment(id, sample, output, snps))

        self.assertEqual(expected_args_str, actual_args_str)

        sample_interleaved = "/path/to/sample/sample.ifq"
        expected_args[1] = sample_interleaved
        expected_args += ["-force_load_ifastq"]
        expected_args_str = " ".join(expected_args)
        actual_args_str = " ".join(
            bwa_aligner.alignment(id, sample_interleaved, "/path/to/sample/chr1_aligned.sam", snps))
        self.assertEqual(expected_args_str, actual_args_str)
