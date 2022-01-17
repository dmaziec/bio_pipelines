import glob
import os
from airflow.models import Variable


def get_aligner():
    aligner = Variable.get('bio_pipe_aligner')
    reference = Variable.get('bio_pipe_reference')
    if aligner == "bowtie2":
        return Bowtie2Aligner(reference=reference)
    if aligner == "bwa":
        return BwaAligner(reference=reference)


def get_index_command():
    aligner = get_aligner()
    command = ['docker', 'run', '--rm', '-v']
    command += [Variable.get('NGS_workspace') + ":" + Variable.get('NGS_workspace')]
    command += [aligner.get_docker_image()]
    command += [aligner.get_docker_index_command()]

    return " ".join(command)


class Aligner:

    def __init__(self,
                 reference,
                 docker_image):
        if reference.endswith(".fasta") is False and reference.endswith(".fa") is False and reference.endswith(
                ".fna") is False:
            raise Exception("Reference path has not reference extension")
        self.docker_image = docker_image
        self.reference = reference
        self.reference_without_ext = self.reference.split(".")[0]
        print(self.reference_without_ext)
        self.index = os.path.basename(self.reference)
        self.reference_location = os.path.dirname(self.reference)

    def alignment(self, id, sample, output, reference, index, snps, additional_args=None, *args, **kwargs):
        pass

    def index_exists(self):
        pass

    def get_docker_index_command(self):
        pass

    def get_docker_image(self):
        return self.docker_image


class Bowtie2Aligner(Aligner):
    def __init__(self,
                 *args, **kwargs):
        super().__init__(docker_image="quay.io/biocontainers/bowtie2:2.4.2--py38h1c8e9b9_0", *args, **kwargs)

    def alignment(self, id, sample, output, snps, additional_args=None, *args, **kwargs):
        if sample.endswith(".ifq"):
            app_args = ["bowtie2Pipeline"]
        else:
            app_args = ["SingleEndBowtie2Pipeline"]

        app_args += [sample]
        app_args += [output]
        app_args += ["-prefix", id]
        app_args += ["-reference", self.reference]
        app_args += ['-index', self.reference_without_ext]
        app_args += ['-sample_id', id]
        app_args += ['-known_snps']
        app_args += [snps]
        app_args += ['-use_docker']

        if additional_args is not None:
            app_args += additional_args

        return app_args

    def index_exists(self):
        if not ((glob.glob(self.reference_without_ext + ".*.bt2") and glob.glob(
                self.reference_without_ext + ".rev.*.bt2"))
                or (glob.glob(self.reference_without_ext + ".*.bt2l") and glob.glob(
                    self.reference_without_ext + ".rev.*.bt2l"))):
            return False
        else:
            return True

    def get_docker_index_command(self):
        return "bowtie2-build " + self.reference + " " + self.reference_without_ext + " && touch " + self.reference_without_ext


class BwaAligner(Aligner):

    def __init__(self,
                 *args, **kwargs):
        super().__init__(docker_image="quay.io/biocontainers/bwa:0.7.17--hed695b0_7", *args, **kwargs)

    def alignment(self, id, sample, output, snps, additional_args=None, *args, **kwargs):
        app_args = ['bwaPipeline']
        app_args += [sample]
        app_args += [output]
        app_args += ['-use_docker']
        app_args += ['-prefix']
        app_args += [id]
        app_args += ['-sample_id']
        app_args += [id]
        app_args += ['-index']
        app_args += [self.reference]
        app_args += ['-reference']
        app_args += [self.reference]
        app_args += ['-known_snps']
        app_args += [snps]
        if sample.endswith(".ifq"):
            app_args += ['-force_load_ifastq']

        if additional_args is not None:
            app_args += additional_args

        return app_args

    def index_exists(self):
        index_ext = [".amb",
                     ".ann",
                     ".bwt",
                     ".sa",
                     ".pac"]
        for ext in index_ext:
            if not glob.glob(self.reference + ext):
                return False
        return True

    def get_docker_index_command(self):
        return "/bin/bash -c \"cd " + self.reference_location + " && bwa index " + self.reference + "\""
