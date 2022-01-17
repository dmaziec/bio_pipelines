import unittest

from airflow import settings
from airflow.models import DagBag
from bio_pipelines.helpers.cannoli_commands_args import *

session = settings.Session()
# export AIRFLOW__CORE__UNIT_TEST_MODE=true
from pathlib import Path
from airflow.models import Variable

source_path = Path(__file__).resolve()
source_dir = source_path.parent


class TestBioDNASeqDAG(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        Variable.set("bio_pipe_aligner", "bowtie2")
        Variable.set("NGS_workspace", str(source_dir.absolute()) + "/resources/ngs_workspace_1/")
        cls.dagbag = DagBag()

    def test_dag_loaded(self):
        dag = self.dagbag.get_dag(dag_id='BioDNASeq')
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 7)

    def test_dependencies_move_to_hdfs(self):
        dag = self.dagbag.get_dag("BioDNASeq")
        move_to_hdfs_task = dag.get_task('move_fromLocal_toHdfs')
        upstream_task_ids = list(map(lambda task: task.task_id, move_to_hdfs_task.upstream_list))
        upstream_task_ids.sort()
        expected = ['index', 'should_index']
        expected.sort()
        self.assertListEqual(upstream_task_ids, expected)
        downstream_task_ids = list(map(lambda task: task.task_id, move_to_hdfs_task.downstream_list))
        downstream_task_ids.sort()
        self.assertListEqual(downstream_task_ids,
                             ['Alignment'])

    def indexing(self):
        dag = self.dagbag.get_dag("BioDNASeq")
        indexing = dag.get_task('should_index')
        upstream_task_ids = list(map(lambda task: task.task_id, indexing.upstream_list))
        upstream_task_ids.sort()
        self.assertListEqual(upstream_task_ids, [])

        self.assertListEqual(indexing.python_callable('False'), ['index', 'move_fromLocal_toHdfs'])
        self.assertEqual(indexing.python_callable('True'), 'move_fromLocal_toHdfs')

        downstream_task_ids = list(map(lambda task: task.task_id, indexing.downstream_list))
        downstream_task_ids.sort()
        self.assertListEqual(downstream_task_ids, ['index', 'move_fromLocal_toHdfs'])

        index = dag.get_task('index')
        upstream_task_ids = list(map(lambda task: task.task_id, index.upstream_list))
        upstream_task_ids.sort()
        self.assertListEqual(upstream_task_ids, ['should_index'])

        downstream_task_ids = list(map(lambda task: task.task_id, index.downstream_list))
        downstream_task_ids.sort()
        self.assertListEqual(downstream_task_ids, ['move_fromLocal_toHdfs'])

    def test_alignment(self):
        dag = self.dagbag.get_dag("BioDNASeq")
        alignment = dag.get_task('Alignment')
        upstream_task_ids = list(map(lambda task: task.task_id, alignment.upstream_list))
        upstream_task_ids.sort()
        self.assertListEqual(upstream_task_ids, ['move_fromLocal_toHdfs'])

        downstream_task_ids = list(map(lambda task: task.task_id, alignment.downstream_list))
        downstream_task_ids.sort()
        self.assertListEqual(downstream_task_ids, ['Variant_Calling'])

    def test_variant_calling(self):
        dag = self.dagbag.get_dag("BioDNASeq")

        alignment = dag.get_task('Variant_Calling')
        upstream_task_ids = list(map(lambda task: task.task_id, alignment.upstream_list))
        upstream_task_ids.sort()
        self.assertListEqual(upstream_task_ids, ['Alignment'])

        downstream_task_ids = list(map(lambda task: task.task_id, alignment.downstream_list))
        downstream_task_ids.sort()
        self.assertListEqual(downstream_task_ids, ['Annotation'])

    def test_annotation(self):
        dag = self.dagbag.get_dag("BioDNASeq")
        alignment = dag.get_task('Annotation')
        upstream_task_ids = list(map(lambda task: task.task_id, alignment.upstream_list))
        upstream_task_ids.sort()
        self.assertListEqual(upstream_task_ids, ['Variant_Calling'])

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
            source_dir.absolute()) + "/resources/ngs_workspace_1/reference/reference" + " && touch " + str(
            source_dir.absolute()) + "/resources/ngs_workspace_1/reference/reference")

    def test_get_index_command_macro_Bowtie2(self):
        Variable.set('bio_pipe_reference',
                     str(source_dir.absolute()) + "/resources/ngs_workspace_1/reference/reference.fa")
        command = get_index_command()
        directory = str(source_dir.absolute()) + "/resources/ngs_workspace_1/"
        fasta = str(source_dir.absolute()) + "/resources/ngs_workspace_1/reference/reference.fa"
        index = str(source_dir.absolute()) + "/resources/ngs_workspace_1/reference/reference"
        expected = ['docker', 'run', '--rm', '-v']
        expected += [Variable.get('NGS_workspace') + ":" + Variable.get('NGS_workspace')]
        expected += ["quay.io/biocontainers/bowtie2:2.4.2--py38h1c8e9b9_0"]
        expected += ["bowtie2-build " + fasta + " " + index + " && touch " + index]
        self.assertEqual(command, " ".join(expected))

    def test_AlignerReferenceWithoutExtension(self):
        with self.assertRaises(Exception) as exc:
            Aligner(reference="/home/reference", docker_image="dummy")
        err = exc.exception
        self.assertEqual(str(err), "Reference path has not reference extension")

    def testMissingIndexBowtie2(self):
        bowtie2_aligner = Bowtie2Aligner(
            reference=str(source_dir.absolute()) + '/resources/ngs_workspace_2/reference/HLA_DQB1_05_01_01_02.fa')
        self.assertFalse(bowtie2_aligner.index_exists())

    def testMissingIndexBwa(self):
        bwa_aligner = Bowtie2Aligner(
            reference=str(source_dir.absolute()) + '/resources/ngs_workspace_2/reference/HLA_DQB1_05_01_01_02.fa')
        self.assertFalse(bwa_aligner.index_exists())

# suite1 = unittest.TestLoader().loadTestsFromTestCase(TestBioDNASeqDAG)
# unittest.TextTestRunner(verbosity=3).run(suite1)
